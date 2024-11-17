/*!
 * Defines the different [Operation]s on a [Table].
*/

use std::{collections::HashMap, sync::Arc};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::PhysicalExpr;
use futures::lock::Mutex;
use futures::{stream, StreamExt, TryStreamExt};
use iceberg_rust_spec::error::Error as SpecError;
use iceberg_rust_spec::manifest_list::{
    manifest_list_schema_v1, manifest_list_schema_v2, Content, FieldSummary, ManifestListEntry,
};
use iceberg_rust_spec::materialized_view_metadata::{depends_on_tables_to_string, SourceTable};
use iceberg_rust_spec::snapshot::DEPENDS_ON_TABLES;
use iceberg_rust_spec::spec::table_metadata::TableMetadata;
use iceberg_rust_spec::spec::{
    manifest::{partition_value_schema, DataFile, ManifestEntry, Status},
    schema::Schema,
    snapshot::{
        generate_snapshot_id, SnapshotBuilder, SnapshotReference, SnapshotRetention, Summary,
    },
};

use iceberg_rust_spec::table_metadata::FormatVersion;
use iceberg_rust_spec::types::StructField;
use iceberg_rust_spec::util::strip_prefix;
use iceberg_rust_spec::values::{Struct, Value};
use object_store::ObjectStore;
use smallvec::SmallVec;

use super::append::{
    select_manifest_partitioned, select_manifest_unpartitioned, split_datafiles, SelectedManifest,
};
use crate::table::manifest::{ManifestReader, ManifestWriter};
use crate::table::manifest_list::ManifestListReader;
use crate::table::pruning_statistics::PruneDataFiles;
use crate::table::Table;
use crate::{
    catalog::commit::{TableRequirement, TableUpdate},
    error::Error,
    util::{partition_struct_to_vec, summary_to_rectangle, Rectangle},
};

/// The target number of datafiles per manifest is dynamic, but we don't want to go below this number.
static MIN_DATAFILES_PER_MANIFEST: usize = 4;

#[derive(Debug)]
///Table operations
pub enum Operation {
    /// Update schema
    AddSchema(Schema),
    /// Update spec
    SetDefaultSpec(i32),
    /// Update table properties
    UpdateProperties(Vec<(String, String)>),
    /// Set Ref
    SetSnapshotRef((String, SnapshotReference)),
    /// Replace the sort order
    // ReplaceSortOrder,
    // /// Update the table location
    // UpdateLocation,
    /// Append new files to the table
    Append {
        branch: Option<String>,
        files: Vec<DataFile>,
        // additional_summary: Option<HashMap<String, String>>,
        lineage: Option<Vec<SourceTable>>,
    },
    // /// Quickly append new files to the table
    // NewFastAppend {
    //     paths: Vec<String>,
    //     partition_values: Vec<Struct>,
    // },
    // /// Replace files in the table and commit
    Rewrite {
        branch: Option<String>,
        files: Vec<DataFile>,
        // additional_summary: Option<HashMap<String, String>>,
        lineage: Option<Vec<SourceTable>>,
    },
    // /// Replace manifests files and commit
    // RewriteManifests,
    /// /// Delete files in the table, based on a filter
    /// and Commit. This is a precursor to the DELETE, UPDATE operations
    Filter {
        branch: Option<String>,
        filter: Option<Arc<dyn PhysicalExpr>>,
        lineage: Option<Vec<SourceTable>>,
        new_files: Vec<DataFile>,
    },
    // /// Remove or replace rows in existing data files
    // NewRowDelta,
    // /// Delete files in the table and commit
    // NewDelete,
    // /// Expire snapshots in the table
    // ExpireSnapshots,
    // /// Manage snapshots in the table
    // ManageSnapshots,
    // /// Read and write table data and metadata files
    // IO,
}

impl Operation {
    pub async fn execute(
        self,
        table: &Table,
        table_metadata: &TableMetadata,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<(Option<TableRequirement>, Vec<TableUpdate>), Error> {
        match self {
            Operation::Append {
                branch,
                files: new_files,
                // additional_summary,
                lineage,
            } => {
                let partition_fields =
                    table_metadata.current_partition_fields(branch.as_deref())?;
                let schema = table_metadata.current_schema(branch.as_deref())?;
                let old_snapshot = table_metadata.current_snapshot(branch.as_deref())?;

                let partition_column_names = partition_fields
                    .iter()
                    .map(|x| x.name())
                    .collect::<SmallVec<[_; 4]>>();

                let bounding_partition_values = new_files
                    .iter()
                    .try_fold(None, |acc, x| {
                        let node = partition_struct_to_vec(x.partition(), &partition_column_names)?;
                        let Some(mut acc) = acc else {
                            return Ok::<_, Error>(Some(Rectangle::new(node.clone(), node)));
                        };
                        acc.expand_with_node(node);
                        Ok(Some(acc))
                    })?
                    .ok_or(Error::NotFound(
                        "Bounding".to_owned(),
                        "rectangle".to_owned(),
                    ))?;

                let manifest_list_schema = match table_metadata.format_version {
                    FormatVersion::V1 => manifest_list_schema_v1(),
                    FormatVersion::V2 => manifest_list_schema_v2(),
                };

                let mut manifest_list_writer =
                    apache_avro::Writer::new(manifest_list_schema, Vec::new());

                let old_manifest_list_location = old_snapshot.map(|x| x.manifest_list()).cloned();

                // Find a manifest to add the new datafiles
                let mut existing_file_count = 0;
                let selected_manifest_opt = if let Some(old_manifest_list_location) =
                    &old_manifest_list_location
                {
                    let old_manifest_list_bytes = object_store
                        .get(&strip_prefix(old_manifest_list_location).as_str().into())
                        .await?
                        .bytes()
                        .await?;

                    let manifest_list_reader =
                        ManifestListReader::new(old_manifest_list_bytes.as_ref(), table_metadata)?;

                    let SelectedManifest {
                        manifest,
                        file_count_all_entries,
                    } = if partition_column_names.is_empty() {
                        select_manifest_unpartitioned(
                            manifest_list_reader,
                            &mut manifest_list_writer,
                        )?
                    } else {
                        select_manifest_partitioned(
                            manifest_list_reader,
                            &mut manifest_list_writer,
                            &bounding_partition_values,
                        )?
                    };
                    existing_file_count = file_count_all_entries;
                    Some(manifest)
                } else {
                    // If manifest list doesn't exist, there is no manifest
                    None
                };

                let selected_manifest_file_count = selected_manifest_opt
                    .as_ref()
                    .and_then(|selected_manifest| {
                        match (
                            selected_manifest.existing_files_count,
                            selected_manifest.added_files_count,
                        ) {
                            (Some(x), Some(y)) => Some(x + y),
                            (Some(x), None) => Some(x),
                            (None, Some(y)) => Some(y),
                            (None, None) => None,
                        }
                    })
                    .unwrap_or(0) as usize;

                let n_splits = compute_n_splits(
                    existing_file_count,
                    new_files.len(),
                    selected_manifest_file_count,
                );

                let bounds = selected_manifest_opt
                    .as_ref()
                    .and_then(|x| x.partitions.as_deref())
                    .map(summary_to_rectangle)
                    .transpose()?
                    .map(|mut x| {
                        x.expand(&bounding_partition_values);
                        x
                    })
                    .unwrap_or(bounding_partition_values);

                let snapshot_id = generate_snapshot_id();
                let snapshot_uuid = &uuid::Uuid::new_v4().to_string();

                let new_datafile_iter = new_files.into_iter().map(|data_file| {
                    ManifestEntry::builder()
                        .with_format_version(table_metadata.format_version)
                        .with_status(Status::Added)
                        .with_snapshot_id(snapshot_id)
                        .with_data_file(data_file)
                        .build()
                        .map_err(crate::spec::error::Error::from)
                        .map_err(Error::from)
                });

                let manifest_schema = ManifestEntry::schema(
                    &partition_value_schema(&partition_fields)?,
                    &table_metadata.format_version,
                )?;

                let new_manifest_list_location = table_metadata.location.to_string()
                    + "/metadata/snap-"
                    + &snapshot_id.to_string()
                    + "-"
                    + snapshot_uuid
                    + ".avro";

                // Write manifest files
                // Split manifest file if limit is exceeded
                if n_splits == 0 {
                    let mut manifest_writer = if let Some(manifest) = selected_manifest_opt {
                        let manifest_bytes: Vec<u8> = object_store
                            .get(&strip_prefix(&manifest.manifest_path).as_str().into())
                            .await?
                            .bytes()
                            .await?
                            .into();

                        ManifestWriter::from_existing(
                            &manifest_bytes,
                            manifest,
                            &manifest_schema,
                            table_metadata,
                            branch.as_deref(),
                        )?
                    } else {
                        let manifest_location = table_metadata.location.to_string()
                            + "/metadata/"
                            + snapshot_uuid
                            + "-m"
                            + &0.to_string()
                            + ".avro";

                        ManifestWriter::new(
                            &manifest_location,
                            snapshot_id,
                            &manifest_schema,
                            table_metadata,
                            branch.as_deref(),
                        )?
                    };

                    for manifest_entry in new_datafile_iter {
                        manifest_writer.append(manifest_entry?)?;
                    }

                    let manifest = manifest_writer.finish(object_store.clone()).await?;

                    manifest_list_writer.append_ser(manifest)?;
                } else {
                    // Split datafiles
                    let splits = if let Some(manifest) = selected_manifest_opt {
                        let manifest_bytes: Vec<u8> = object_store
                            .get(&strip_prefix(&manifest.manifest_path).as_str().into())
                            .await?
                            .bytes()
                            .await?
                            .into();

                        let manifest_reader = ManifestReader::new(&*manifest_bytes)?
                            .map(|x| x.map_err(Error::from))
                            .map(|entry| {
                                let mut entry = entry?;
                                *entry.status_mut() = Status::Existing;
                                if entry.sequence_number().is_none() {
                                    *entry.sequence_number_mut() =
                                        table_metadata.sequence_number(entry.snapshot_id().ok_or(
                                            apache_avro::Error::DeserializeValue(
                                                "Snapshot_id missing in Manifest Entry.".to_owned(),
                                            ),
                                        )?);
                                }
                                Ok(entry)
                            });

                        split_datafiles(
                            new_datafile_iter.chain(manifest_reader),
                            bounds,
                            &partition_column_names,
                            n_splits,
                        )?
                    } else {
                        split_datafiles(
                            new_datafile_iter,
                            bounds,
                            &partition_column_names,
                            n_splits,
                        )?
                    };

                    for (i, entries) in splits.into_iter().enumerate() {
                        let manifest_location = table_metadata.location.to_string()
                            + "/metadata/"
                            + snapshot_uuid
                            + "-m"
                            + &i.to_string()
                            + ".avro";

                        let mut manifest_writer = ManifestWriter::new(
                            &manifest_location,
                            snapshot_id,
                            &manifest_schema,
                            table_metadata,
                            branch.as_deref(),
                        )?;

                        for manifest_entry in entries {
                            manifest_writer.append(manifest_entry)?;
                        }

                        let manifest = manifest_writer.finish(object_store.clone()).await?;

                        manifest_list_writer.append_ser(manifest)?;
                    }
                };

                let manifest_list_bytes = manifest_list_writer.into_inner()?;

                object_store
                    .put(
                        &strip_prefix(&new_manifest_list_location).into(),
                        manifest_list_bytes.into(),
                    )
                    .await?;

                let mut snapshot_builder = SnapshotBuilder::default();
                snapshot_builder
                    .with_snapshot_id(snapshot_id)
                    .with_manifest_list(new_manifest_list_location)
                    .with_sequence_number(
                        old_snapshot
                            .map(|x| *x.sequence_number() + 1)
                            .unwrap_or_default(),
                    )
                    .with_summary(Summary {
                        operation: iceberg_rust_spec::spec::snapshot::Operation::Append,
                        other: if let Some(lineage) = lineage {
                            HashMap::from_iter(vec![(
                                DEPENDS_ON_TABLES.to_owned(),
                                depends_on_tables_to_string(&lineage)?,
                            )])
                        } else {
                            HashMap::new()
                        },
                    })
                    .with_schema_id(*schema.schema_id());
                let snapshot = snapshot_builder
                    .build()
                    .map_err(iceberg_rust_spec::error::Error::from)?;

                Ok((
                    old_snapshot.map(|x| TableRequirement::AssertRefSnapshotId {
                        r#ref: branch.clone().unwrap_or("main".to_owned()),
                        snapshot_id: *x.snapshot_id(),
                    }),
                    vec![
                        TableUpdate::AddSnapshot { snapshot },
                        TableUpdate::SetSnapshotRef {
                            ref_name: branch.unwrap_or("main".to_owned()),
                            snapshot_reference: SnapshotReference {
                                snapshot_id,
                                retention: SnapshotRetention::default(),
                            },
                        },
                    ],
                ))
            }
            Operation::Rewrite {
                branch,
                files,
                // additional_summary,
                lineage,
            } => {
                let table_metadata = table.metadata();
                let object_store = table.object_store();
                let partition_fields =
                    table_metadata.current_partition_fields(branch.as_deref())?;
                let old_snapshot = table_metadata.current_snapshot(branch.as_deref())?;
                let schema = table_metadata.current_schema(branch.as_deref())?.clone();

                let partition_column_names = partition_fields
                    .iter()
                    .map(|x| x.name())
                    .collect::<SmallVec<[_; 4]>>();

                let bounding_partition_values = files
                    .iter()
                    .try_fold(None, |acc, x| {
                        let node = partition_struct_to_vec(x.partition(), &partition_column_names)?;
                        let Some(mut acc) = acc else {
                            return Ok::<_, Error>(Some(Rectangle::new(node.clone(), node)));
                        };
                        acc.expand_with_node(node);
                        Ok(Some(acc))
                    })?
                    .ok_or(Error::NotFound(
                        "Bounding".to_owned(),
                        "rectangle".to_owned(),
                    ))?;

                let manifest_list_schema = match table_metadata.format_version {
                    FormatVersion::V1 => manifest_list_schema_v1(),
                    FormatVersion::V2 => manifest_list_schema_v2(),
                };

                let mut manifest_list_writer =
                    apache_avro::Writer::new(manifest_list_schema, Vec::new());

                let n_splits = compute_n_splits(0, files.len(), 0);

                let snapshot_id = generate_snapshot_id();
                let sequence_number = table_metadata.last_sequence_number + 1;

                let new_datafile_iter = files.into_iter().map(|data_file| {
                    ManifestEntry::builder()
                        .with_format_version(table_metadata.format_version)
                        .with_status(Status::Added)
                        .with_snapshot_id(snapshot_id)
                        .with_sequence_number(sequence_number)
                        .with_data_file(data_file)
                        .build()
                        .map_err(crate::spec::error::Error::from)
                        .map_err(Error::from)
                });

                let manifest_schema = ManifestEntry::schema(
                    &partition_value_schema(&partition_fields)?,
                    &table_metadata.format_version,
                )?;

                let snapshot_uuid = &uuid::Uuid::new_v4().to_string();
                let new_manifest_list_location = table_metadata.location.to_string()
                    + "/metadata/snap-"
                    + &snapshot_id.to_string()
                    + "-"
                    + snapshot_uuid
                    + ".avro";

                // Write manifest files
                // Split manifest file if limit is exceeded
                if n_splits == 0 {
                    // If manifest doesn't need to be split

                    let manifest_location = table_metadata.location.to_string()
                        + "/metadata/"
                        + snapshot_uuid
                        + "-m"
                        + &0.to_string()
                        + ".avro";
                    let mut manifest_writer = ManifestWriter::new(
                        &manifest_location,
                        snapshot_id,
                        &manifest_schema,
                        table_metadata,
                        branch.as_deref(),
                    )?;

                    for manifest_entry in new_datafile_iter {
                        manifest_writer.append(manifest_entry?)?;
                    }

                    let manifest = manifest_writer.finish(object_store.clone()).await?;

                    manifest_list_writer.append_ser(manifest)?;
                } else {
                    // Split datafiles
                    let splits = split_datafiles(
                        new_datafile_iter,
                        bounding_partition_values,
                        &partition_column_names,
                        n_splits,
                    )?;

                    for (i, entries) in splits.into_iter().enumerate() {
                        let manifest_location = table_metadata.location.to_string()
                            + "/metadata/"
                            + snapshot_uuid
                            + "-m"
                            + &i.to_string()
                            + ".avro";

                        let mut manifest_writer = ManifestWriter::new(
                            &manifest_location,
                            snapshot_id,
                            &manifest_schema,
                            table_metadata,
                            branch.as_deref(),
                        )?;

                        for manifest_entry in entries {
                            manifest_writer.append(manifest_entry)?;
                        }

                        let manifest = manifest_writer.finish(object_store.clone()).await?;

                        manifest_list_writer.append_ser(manifest)?;
                    }
                };

                let manifest_list_bytes = manifest_list_writer.into_inner()?;

                object_store
                    .put(
                        &strip_prefix(&new_manifest_list_location).into(),
                        manifest_list_bytes.into(),
                    )
                    .await?;

                let mut snapshot_builder = SnapshotBuilder::default();
                snapshot_builder
                    .with_snapshot_id(snapshot_id)
                    .with_sequence_number(0)
                    .with_schema_id(*schema.schema_id())
                    .with_manifest_list(new_manifest_list_location)
                    .with_summary(Summary {
                        operation: iceberg_rust_spec::spec::snapshot::Operation::Append,
                        other: if let Some(lineage) = lineage {
                            HashMap::from_iter(vec![(
                                DEPENDS_ON_TABLES.to_owned(),
                                depends_on_tables_to_string(&lineage)?,
                            )])
                        } else {
                            HashMap::new()
                        },
                    });
                let snapshot = snapshot_builder
                    .build()
                    .map_err(iceberg_rust_spec::error::Error::from)?;

                let old_snapshot_ids: Vec<i64> =
                    table_metadata.snapshots.keys().map(Clone::clone).collect();

                Ok((
                    old_snapshot.map(|x| TableRequirement::AssertRefSnapshotId {
                        r#ref: branch.clone().unwrap_or("main".to_owned()),
                        snapshot_id: *x.snapshot_id(),
                    }),
                    vec![
                        TableUpdate::RemoveSnapshots {
                            snapshot_ids: old_snapshot_ids,
                        },
                        TableUpdate::AddSnapshot { snapshot },
                        TableUpdate::SetSnapshotRef {
                            ref_name: branch.unwrap_or("main".to_owned()),
                            snapshot_reference: SnapshotReference {
                                snapshot_id,
                                retention: SnapshotRetention::default(),
                            },
                        },
                    ],
                ))
            }
            Operation::UpdateProperties(entries) => Ok((
                None,
                vec![TableUpdate::SetProperties {
                    updates: HashMap::from_iter(entries),
                }],
            )),
            Operation::SetSnapshotRef((key, value)) => Ok((
                table_metadata
                    .refs
                    .get(&key)
                    .map(|x| TableRequirement::AssertRefSnapshotId {
                        r#ref: key.clone(),
                        snapshot_id: x.snapshot_id,
                    }),
                vec![TableUpdate::SetSnapshotRef {
                    ref_name: key,
                    snapshot_reference: value,
                }],
            )),
            Operation::AddSchema(schema) => {
                let last_column_id = schema.fields().iter().map(|x| x.id).max();
                Ok((
                    None,
                    vec![TableUpdate::AddSchema {
                        schema,
                        last_column_id,
                    }],
                ))
            }
            Operation::SetDefaultSpec(spec_id) => {
                Ok((None, vec![TableUpdate::SetDefaultSpec { spec_id }]))
            }
            Operation::Filter {
                branch,
                filter,
                lineage,
                new_files,
            } => {
                // Delete Manifests, files based on the filter provided.
                // use code similar to table_scan(), and NewAppend() to generate code which does the following
                // 1. get the manifest list
                // 2. for each manifest, get the files
                // 3. for each file, check if it passes the filter
                // 4. if it passes the filter, don't add it to the new manifest list
                // 5. if it doesn't pass the filter, add it to the manifest list of the table
                // 6. Create a snapshot for the same
                let schema = table_metadata.current_schema(branch.as_deref())?;
                let old_snapshot = table_metadata.current_snapshot(branch.as_deref())?;
                let manifests = if let Some(snapshot) = old_snapshot {
                    snapshot
                        .manifests(table_metadata, object_store.clone())
                        .await?
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    vec![]
                };
                let all_datafiles = table
                    .datafiles(&manifests, None)
                    .await
                    .map_err(Into::<Error>::into)?;
                let pruned_data_files = if let Some(physical_predicate) = filter.clone() {
                    let arrow_schema: SchemaRef = Arc::new((schema.fields()).try_into().unwrap());
                    let pruning_predicate =
                        match PruningPredicate::try_new(physical_predicate, arrow_schema.clone()) {
                            Ok(predicate) => predicate,
                            Err(e) => {
                                return Err(Error::IO(e.into()));
                            }
                        };
                    let files_to_prune = match pruning_predicate.prune(&PruneDataFiles::new(
                        &schema,
                        &arrow_schema,
                        &all_datafiles,
                    )) {
                        Ok(files) => files,
                        Err(e) => {
                            return Err(Error::IO(e.into()));
                        }
                    };
                    let pruned_data_files = all_datafiles
                        .clone()
                        .into_iter()
                        .zip(files_to_prune.into_iter())
                        .filter_map(
                            |(data_file, should_prune)| {
                                if should_prune {
                                    None
                                } else {
                                    Some(data_file)
                                }
                            },
                        )
                        .collect::<Vec<_>>();

                    pruned_data_files
                } else {
                    all_datafiles.clone()
                };
                let files: Vec<DataFile> = pruned_data_files
                    .into_iter()
                    .map(|manifest| {
                        let partition_values = manifest.data_file();
                        partition_values.clone()
                    })
                    .collect();
                // append new_files to files
                let mut files = files.clone();
                files.append(&mut new_files.clone());

                // Split datafils by partition
                let datafiles = Arc::new(files.clone().into_iter().map(Ok::<_, Error>).try_fold(
                    HashMap::<Struct, Vec<DataFile>>::new(),
                    |mut acc, x| {
                        let x = x?;
                        let partition_value = x.partition().clone();
                        acc.entry(partition_value).or_default().push(x);
                        Ok::<_, Error>(acc)
                    },
                )?);
                let snapshot_id = generate_snapshot_id();
                let manifest_list_location = table_metadata.location.to_string()
                    + "/metadata/snap-"
                    + &snapshot_id.to_string()
                    + "-"
                    + &uuid::Uuid::new_v4().to_string()
                    + ".avro";
                if datafiles.len() > 0 {
                    let manifest_iter = datafiles.keys().enumerate().map(|(i, partition_value)| {
                        let manifest_location = manifest_list_location
                            .clone()
                            .to_string()
                            .trim_end_matches(".avro")
                            .to_owned()
                            + "-m"
                            + &(i).to_string()
                            + ".avro";
                        let manifest = ManifestListEntry {
                            format_version: table_metadata.format_version.clone(),
                            manifest_path: manifest_location,
                            manifest_length: 0,
                            partition_spec_id: table_metadata.default_spec_id,
                            content: Content::Data,
                            sequence_number: table_metadata.last_sequence_number,
                            min_sequence_number: 0,
                            added_snapshot_id: snapshot_id,
                            added_files_count: Some(0),
                            existing_files_count: Some(0),
                            deleted_files_count: Some(0),
                            added_rows_count: Some(0),
                            existing_rows_count: Some(0),
                            deleted_rows_count: Some(0),
                            partitions: None,
                            key_metadata: None,
                        };
                        (ManifestStatus::New(manifest), vec![partition_value.clone()])
                    });
                    let partition_columns = Arc::new(
                        table_metadata
                            .default_partition_spec()?
                            .fields()
                            .iter()
                            .map(|x| schema.fields().get(*x.source_id() as usize))
                            .collect::<Option<Vec<_>>>()
                            .ok_or(Error::InvalidFormat(
                                "Partition column in schema".to_string(),
                            ))?,
                    );
                    let manifest_list_schema =
                        ManifestListEntry::schema(&table_metadata.format_version)?;
                    let manifest_list_writer = Arc::new(Mutex::new(apache_avro::Writer::new(
                        &manifest_list_schema,
                        Vec::new(),
                    )));
                    stream::iter(manifest_iter)
                        .then(|(manifest, files): (ManifestStatus, Vec<Struct>)| {
                            let object_store = object_store.clone();
                            let datafiles = datafiles.clone();
                            let partition_columns = partition_columns.clone();
                            let branch = branch.clone();
                            let schema = &schema;
                            let old_storage_table_metadata = &table_metadata;
                            async move {
                                write_manifest(
                                    old_storage_table_metadata,
                                    manifest,
                                    files,
                                    datafiles,
                                    schema,
                                    &partition_columns,
                                    object_store,
                                    branch,
                                )
                                .await
                            }
                        })
                        .try_for_each_concurrent(None, |manifest| {
                            let manifest_list_writer = manifest_list_writer.clone();
                            async move {
                                manifest_list_writer.lock().await.append_ser(manifest)?;
                                Ok(())
                            }
                        })
                        .await?;
                    let manifest_list_bytes = Arc::into_inner(manifest_list_writer)
                        .unwrap()
                        .into_inner()
                        .into_inner()?;
                    object_store
                        .put(
                            &strip_prefix(&manifest_list_location.clone()).into(),
                            manifest_list_bytes.into(),
                        )
                        .await?;
                }
                let mut snapshot_builder = SnapshotBuilder::default();
                snapshot_builder
                    .with_snapshot_id(snapshot_id)
                    .with_sequence_number(
                        old_snapshot
                            .map(|x| *x.sequence_number() + 1)
                            .unwrap_or_default(),
                    )
                    .with_schema_id(*schema.schema_id())
                    .with_manifest_list(manifest_list_location)
                    .with_summary(Summary {
                        operation: iceberg_rust_spec::spec::snapshot::Operation::Delete,
                        other: if let Some(lineage) = lineage {
                            HashMap::from_iter(vec![(
                                DEPENDS_ON_TABLES.to_owned(),
                                depends_on_tables_to_string(&lineage)?,
                            )])
                        } else {
                            HashMap::new()
                        },
                    });
                let snapshot = snapshot_builder
                    .build()
                    .map_err(iceberg_rust_spec::error::Error::from)?;

                Ok((
                    old_snapshot.map(|x| TableRequirement::AssertRefSnapshotId {
                        r#ref: branch.clone().unwrap_or("main".to_owned()),
                        snapshot_id: *x.snapshot_id(),
                    }),
                    vec![
                        TableUpdate::AddSnapshot { snapshot },
                        TableUpdate::SetSnapshotRef {
                            ref_name: branch.unwrap_or("main".to_owned()),
                            snapshot_reference: SnapshotReference {
                                snapshot_id,
                                retention: SnapshotRetention::default(),
                            },
                        },
                    ],
                ))
            }
        }
    }
}

/// To achieve fast lookups of the datafiles, the manifest tree should be somewhat balanced, meaning that manifest files should contain a similar number of datafiles.
/// This means that manifest files might need to be split up when they get too large. Since the number of datafiles being added by a append operation might be really large,
/// it might even be required to split the manifest file multiple times. *n_splits* stores how many times a manifest file needs to be split to give at most *limit* datafiles per manifest.
fn compute_n_splits(
    existing_file_count: usize,
    new_file_count: usize,
    selected_manifest_file_count: usize,
) -> u32 {
    // We want:
    //   nb manifests per manifest list ~= nb data files per manifest
    // Since:
    //   total number of data files = nb manifests per manifest list * nb data files per manifest
    // We shall have:
    //   limit = sqrt(total number of data files)
    let limit = MIN_DATAFILES_PER_MANIFEST
        + ((existing_file_count + new_file_count) as f64).sqrt() as usize;
    let new_manifest_file_count = selected_manifest_file_count + new_file_count;
    match new_manifest_file_count / limit {
        0 => 0,
        x => x.ilog2() + 1,
    }
}

pub enum ManifestStatus {
    New(ManifestListEntry),
    Existing(ManifestListEntry),
}

pub(crate) async fn write_manifest(
    table_metadata: &TableMetadata,
    manifest: ManifestStatus,
    files: Vec<Struct>,
    datafiles: Arc<HashMap<Struct, Vec<DataFile>>>,
    schema: &Schema,
    partition_columns: &[&StructField],
    object_store: Arc<dyn ObjectStore>,
    branch: Option<String>,
) -> Result<ManifestListEntry, Error> {
    let manifest_schema = ManifestEntry::schema(
        &partition_value_schema(table_metadata.default_partition_spec()?.fields(), schema)?,
        &table_metadata.format_version,
    )?;

    let mut manifest_writer = ManifestWriter::new(
        Vec::new(),
        &manifest_schema,
        table_metadata,
        branch.as_deref(),
    )?;

    let mut manifest = match manifest {
        ManifestStatus::Existing(manifest) => {
            let manifest_bytes: Vec<u8> = object_store
                .get(&strip_prefix(&manifest.manifest_path).as_str().into())
                .await?
                .bytes()
                .await?
                .into();

            let manifest_reader = apache_avro::Reader::new(&*manifest_bytes)?;
            manifest_writer.extend(manifest_reader.filter_map(Result::ok))?;
            manifest
        }
        ManifestStatus::New(manifest) => manifest,
    };
    let files_count = manifest.added_files_count.unwrap_or_default() + files.len() as i32;
    for path in files {
        for datafile in datafiles.get(&path).ok_or(Error::InvalidFormat(
            "Datafiles for partition value".to_string(),
        ))? {
            let mut added_rows_count = 0;

            if manifest.partitions.is_none() {
                manifest.partitions = Some(
                    table_metadata
                        .default_partition_spec()?
                        .fields()
                        .iter()
                        .map(|_| FieldSummary {
                            contains_null: false,
                            contains_nan: None,
                            lower_bound: None,
                            upper_bound: None,
                        })
                        .collect::<Vec<FieldSummary>>(),
                );
            }

            added_rows_count += datafile.record_count();
            update_partitions(
                manifest.partitions.as_mut().unwrap(),
                datafile.partition(),
                partition_columns,
            )?;

            let manifest_entry = ManifestEntry::builder()
                .with_format_version(table_metadata.format_version.clone())
                .with_status(Status::Added)
                .with_snapshot_id(table_metadata.current_snapshot_id.unwrap())
                .with_sequence_number(
                    table_metadata
                        .current_snapshot(branch.as_deref())?
                        .map(|x| *x.sequence_number())
                        .unwrap(),
                )
                .with_data_file(datafile.clone())
                .build()
                .map_err(SpecError::from)?;

            manifest_writer.append_ser(manifest_entry)?;

            manifest.added_files_count = match manifest.added_files_count {
                Some(count) => Some(count + files_count),
                None => Some(files_count),
            };
            manifest.added_rows_count = match manifest.added_rows_count {
                Some(count) => Some(count + added_rows_count),
                None => Some(added_rows_count),
            };
        }
    }

    let manifest_bytes = manifest_writer.into_inner()?;

    let manifest_length: i64 = manifest_bytes.len() as i64;

    manifest.manifest_length += manifest_length;

    object_store
        .put(
            &strip_prefix(&manifest.manifest_path).as_str().into(),
            manifest_bytes.into(),
        )
        .await?;

    Ok::<_, Error>(manifest)
}

fn update_partitions(
    partitions: &mut [FieldSummary],
    partition_values: &Struct,
    partition_columns: &[&StructField],
) -> Result<(), Error> {
    for (field, summary) in partition_columns.iter().zip(partitions.iter_mut()) {
        let value = &partition_values.fields[*partition_values
            .lookup
            .get(&field.name)
            .ok_or_else(|| Error::InvalidFormat("partition value in schema".to_string()))?];
        if let Some(value) = value {
            if let Some(lower_bound) = &mut summary.lower_bound {
                match (value, lower_bound) {
                    (Value::Int(val), Value::Int(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::LongInt(val), Value::LongInt(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Float(val), Value::Float(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Double(val), Value::Double(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Date(val), Value::Date(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Time(val), Value::Time(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::Timestamp(val), Value::Timestamp(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    (Value::TimestampTZ(val), Value::TimestampTZ(current)) => {
                        if *current > *val {
                            *current = *val
                        }
                    }
                    _ => {}
                }
            }
            if let Some(upper_bound) = &mut summary.upper_bound {
                match (value, upper_bound) {
                    (Value::Int(val), Value::Int(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::LongInt(val), Value::LongInt(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Float(val), Value::Float(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Double(val), Value::Double(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Date(val), Value::Date(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Time(val), Value::Time(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::Timestamp(val), Value::Timestamp(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    (Value::TimestampTZ(val), Value::TimestampTZ(current)) => {
                        if *current < *val {
                            *current = *val
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

/// checks if partition values lie in the bounds of the field summary
fn partition_values_in_bounds<'a>(
    partitions: &[FieldSummary],
    partition_values: impl Iterator<Item = &'a Struct>,
    partition_spec: &[PartitionField],
    schema: &Schema,
) -> Vec<Struct> {
    partition_values
        .filter(|value| {
            partition_spec
                .iter()
                .map(|field| {
                    let name = &schema
                        .fields()
                        .get(*field.source_id() as usize)
                        .ok_or_else(|| {
                            Error::InvalidFormat("partition values in schema".to_string())
                        })
                        .unwrap()
                        .name;
                    value
                        .get(name)
                        .ok_or_else(|| {
                            Error::InvalidFormat("partition values in schema".to_string())
                        })
                        .unwrap()
                })
                .zip(partitions.iter())
                .all(|(value, summary)| {
                    if let Some(value) = value {
                        if let (Some(lower_bound), Some(upper_bound)) =
                            (&summary.lower_bound, &summary.upper_bound)
                        {
                            match (value, lower_bound, upper_bound) {
                                (
                                    Value::Int(val),
                                    Value::Int(lower_bound),
                                    Value::Int(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                (
                                    Value::LongInt(val),
                                    Value::LongInt(lower_bound),
                                    Value::LongInt(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                (
                                    Value::Float(val),
                                    Value::Float(lower_bound),
                                    Value::Float(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                (
                                    Value::Double(val),
                                    Value::Double(lower_bound),
                                    Value::Double(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                (
                                    Value::Date(val),
                                    Value::Date(lower_bound),
                                    Value::Date(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                (
                                    Value::Time(val),
                                    Value::Time(lower_bound),
                                    Value::Time(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                (
                                    Value::Timestamp(val),
                                    Value::Timestamp(lower_bound),
                                    Value::Timestamp(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                (
                                    Value::TimestampTZ(val),
                                    Value::TimestampTZ(lower_bound),
                                    Value::TimestampTZ(upper_bound),
                                ) => *lower_bound <= *val && *upper_bound >= *val,
                                _ => false,
                            }
                        } else {
                            false
                        }
                    } else {
                        summary.contains_null
                    }
                })
        })
        .map(Clone::clone)
        .collect()
}
