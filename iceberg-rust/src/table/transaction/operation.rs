/*!
 * Defines the different [Operation]s on a [Table].
*/

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use apache_avro::from_value;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use futures::{lock::Mutex, stream, StreamExt, TryStreamExt};
use datafusion_expr::FilterOp;
use iceberg_rust_spec::spec::{
    manifest::{partition_value_schema, Content, DataFile, ManifestEntry, ManifestWriter, Status},
    manifest_list::{FieldSummary, ManifestListEntry, ManifestListEntryEnum},
    materialized_view_metadata::{depends_on_tables_to_string, SourceTable},
    partition::PartitionField,
    schema::Schema,
    snapshot::{
        generate_snapshot_id, SnapshotBuilder, SnapshotReference, SnapshotRetention, Summary,
        DEPENDS_ON_TABLES,
    },
    types::StructField,
    values::{Struct, Value},
};
use iceberg_rust_spec::util::strip_prefix;
use iceberg_rust_spec::{
    error::Error as SpecError,
    spec::table_metadata::TableMetadata,
};
use crate::table::Table;
use crate::table::pruning_statistics::PruneDataFiles;
use object_store::ObjectStore;

use crate::{
    catalog::commit::{TableRequirement, TableUpdate},
    error::Error,
};

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
    NewAppend {
        branch: Option<String>,
        files: Vec<DataFile>,
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
        lineage: Option<Vec<SourceTable>>,
    },
    // /// Replace manifests files and commit
    // RewriteManifests,
    /// /// Delete files in the table, based on a filter
    /// and Commit. This is a precursor to the DELETE, UPDATE operations
    Filter{
        branch: Option<String>,
        filter: Option<Arc<dyn PhysicalExpr>>,
        lineage: Option<Vec<SourceTable>>,
        new_files: Vec<DataFile>,
        op: FilterOp
    },
    // /// Remove or replace rows in existing data files
    // NewRowDelta,
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
            Operation::NewAppend {
                branch,
                files,
                lineage,
            } => {
                let partition_spec = table_metadata.default_partition_spec()?;
                let schema = table_metadata.current_schema(branch.as_deref())?;
                let old_snapshot = table_metadata.current_snapshot(branch.as_deref())?;

                let datafiles = Arc::new(
                    files
                    .into_iter()
                    .map(Ok::<_, Error>)
                    .try_fold(
                        HashMap::<Struct, Vec<DataFile>>::new(),
                        |mut acc, x| {
                            let x = x?;
                            let partition_value = x.partition().clone();
                            acc.entry(partition_value).or_default().push(x);
                            Ok::<_, Error>(acc)
                        },
                    )?
                );

                let manifest_list_schema =
                    ManifestListEntry::schema(&table_metadata.format_version)?;

                let manifest_list_writer = Arc::new(Mutex::new(apache_avro::Writer::new(
                    &manifest_list_schema,
                    Vec::new(),
                )));

                let existing_partitions = Arc::new(Mutex::new(HashSet::new()));

                let old_manifest_list_location = old_snapshot.map(|x| x.manifest_list()).cloned();

                let manifest_list_bytes = match old_manifest_list_location {
                    Some(old_manifest_list_location) =>{
                        match object_store.get(&strip_prefix(&old_manifest_list_location).as_str().into()).await {
                            Ok(manifest) => {
                                Some(manifest.bytes().await?)
                            },
                            Err(e) => {
                                if !e.to_string().contains("No data in memory found") {
                                    panic!("Error Reading Manifest List {e}");
                                }
                                // TODO :: Find a graceful solution to the empty manifest list problem
                                eprintln!("Proceeding with Empty Manifest List");
                                None
                            }
                        }
                    },
                    None => None,
                };

                // Check if file has content "null", if so manifest_list file is empty
                let existing_manifest_iter = if let Some(manifest_list_bytes) = &manifest_list_bytes
                {
                    let manifest_list_reader =
                        match apache_avro::Reader::new(manifest_list_bytes.as_ref()) {
                            Ok(manifest_list_reader) => manifest_list_reader,
                            Err(e) => {
                                eprintln!("Error Reading Manifest List {}", e);
                                return Err(e.into());
                            }
                        };
                    
                    Some(stream::iter(manifest_list_reader)
                    .filter_map(|manifest| {
                        let datafiles = datafiles.clone();
                        let existing_partitions = existing_partitions.clone();
                        async move {
                            let manifest = manifest
                                .map_err(Into::into)
                                .and_then(|value| {
                                    ManifestListEntry::try_from_enum(
                                        from_value::<ManifestListEntryEnum>(&value)?,
                                        table_metadata,
                                    )
                                })
                                .unwrap();

                            if let Some(summary) = &manifest.partitions {
                                let partition_values = partition_values_in_bounds(
                                    summary,
                                    datafiles.keys(),
                                    partition_spec.fields(),
                                    schema,
                                );
                                if !partition_values.is_empty() {
                                    for file in &partition_values {
                                        existing_partitions.lock().await.insert(file.clone());
                                    }
                                    Some((ManifestStatus::Existing(manifest), partition_values))
                                } else {
                                    Some((ManifestStatus::Existing(manifest), vec![]))
                                }
                            } else {
                                Some((ManifestStatus::Existing(manifest), vec![]))
                            }
                        }
                    }))
                } else {
                    None
                };

                let manifest_count = if let Some(manifest_list_bytes) = &manifest_list_bytes {
                    apache_avro::Reader::new(manifest_list_bytes.as_ref())?.count()
                } else {
                    0
                };

                let snapshot_id = generate_snapshot_id();
                let new_manifest_list_location = table_metadata.location.to_string()
                    + "/metadata/snap-"
                    + &snapshot_id.to_string()
                    + &uuid::Uuid::new_v4().to_string()
                    + ".avro";

                let new_manifest_iter = stream::iter(datafiles.iter().enumerate())
                .filter_map(
                    |(i, (partition_value, _))| {
                        let existing_partitions = existing_partitions.clone();
                        let new_manifest_list_location = new_manifest_list_location.clone();
                        async move {
                            if !existing_partitions.lock().await.contains(partition_value) {
                                let manifest_location = new_manifest_list_location
                                    .to_string()
                                    .trim_end_matches(".avro")
                                    .to_owned()
                                    + "-m"
                                    + &(manifest_count + i).to_string()
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
                                Some((ManifestStatus::New(manifest), vec![partition_value.clone()]))
                            } else {
                                None
                            }
                        }
                    },
                );

                let partition_columns = Arc::new(
                    partition_spec
                        .fields()
                        .iter()
                        .map(|x| schema.fields().get(*x.source_id() as usize))
                        .collect::<Option<Vec<_>>>()
                        .ok_or(Error::InvalidFormat(
                            "Partition column in schema".to_string(),
                        ))?,
                );

                match existing_manifest_iter {
                    Some(existing_manifest_iter) => {
                        let manifest_iter =
                            Box::new(existing_manifest_iter.chain(new_manifest_iter));

                        manifest_iter
                            .then(|(manifest, files): (ManifestStatus, Vec<Struct>)| {
                                let object_store = object_store.clone();
                                let datafiles = datafiles.clone();
                                let partition_columns = partition_columns.clone();
                                let branch = branch.clone();
                                async move {
                                    write_manifest(
                                        table_metadata,
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
                    }
                    None => {
                        new_manifest_iter
                            .then(|(manifest, files): (ManifestStatus, Vec<Struct>)| {
                                let object_store = object_store.clone();
                                let datafiles = datafiles.clone();
                                let partition_columns = partition_columns.clone();
                                let branch = branch.clone();
                                async move {
                                    write_manifest(
                                        table_metadata,
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
                    }
                }

                let manifest_list_bytes = Arc::into_inner(manifest_list_writer)
                    .unwrap()
                    .into_inner()
                    .into_inner()?;

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
            },
            Operation::Rewrite {
                branch,
                files,
                lineage,
            } => {
                let table_metadata = table.metadata();
                let object_store = table.object_store();

                let old_snapshot = table_metadata.current_snapshot(branch.as_deref())?;
                let schema = table_metadata.current_schema(branch.as_deref())?.clone();

                // Split datafils by partition
                let datafiles = Arc::new(files.into_iter().map(Ok::<_, Error>).try_fold(
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

                let manifest_iter = datafiles.keys().enumerate()
                .map(|(i, partition_value)| {
                    let manifest_location = manifest_list_location
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
                        &strip_prefix(&manifest_list_location).into(),
                        manifest_list_bytes.into(),
                    )
                    .await?;

                let mut snapshot_builder = SnapshotBuilder::default();
                snapshot_builder
                    .with_snapshot_id(snapshot_id)
                    .with_sequence_number(0)
                    .with_schema_id(*schema.schema_id())
                    .with_manifest_list(manifest_list_location)
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
            },
            Operation::Filter {
                branch,
                filter,
                lineage,
                new_files,
                op
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

                let manifests: Vec<ManifestListEntry> = if let Some(snapshot) = old_snapshot {
                    match snapshot.manifests(table_metadata, object_store.clone()).await {
                        Ok(stream) => match stream.collect::<Result<Vec<_>, _>>() {
                            Ok(manifests) => manifests,
                            Err(e) => {
                                eprintln!("Error Reading Manifest List during Operation::Filter {e}");
                                vec![]
                            },
                        },
                        Err(e) => {
                                if !e.to_string().contains("No data in memory found") {
                                    panic!("Error Reading Manifest List {e}");
                                }
                                // TODO :: Find a graceful solution to the empty manifest list problem
                                eprintln!("Proceeding with Empty Manifest List");
                                vec![]
                        }
                    }
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

                    let files_to_prune =
                    match pruning_predicate.prune(&PruneDataFiles::new(&schema, &arrow_schema, &all_datafiles)) 
                    {
                        Ok(files) => files,
                        Err(e) => {
                            return Err(Error::IO(e.into()));
                        }
                    };

                    let pruned_data_files = all_datafiles.clone().into_iter()
                    .zip(files_to_prune.into_iter())
                    .filter_map(|(data_file, should_prune)| {
                        if should_prune {
                            None
                        } else {
                            Some(data_file)
                        }
                    })
                    .collect::<Vec<_>>();
                    
                    Some(pruned_data_files)
                } else {
                    // 1. Delete filter will ALWAYS have a filter
                    // 2. If the filter is None. Then just append the new files to the manifest
                    match op {
                        FilterOp::Delete => Some(all_datafiles.clone()),
                        FilterOp::Update => Some(vec![]),
                        FilterOp::Filter => None
                    }
                };

                let files: Vec<DataFile> = pruned_data_files.unwrap()
                .into_iter()
                .map(|manifest| {
                        let partition_values = manifest
                            .data_file();
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
                    let manifest_iter = datafiles
                    .keys()
                    .enumerate()
                    .map(|(i, partition_value)| {
                        let manifest_location = manifest_list_location.clone()
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
                        .unwrap_or_default()
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
            },
            Operation::UpdateProperties(entries) => Ok((
                None,
                vec![TableUpdate::SetProperties {
                    updates: HashMap::from_iter(entries),
                }],
            )),
            Operation::SetSnapshotRef((key, value)) => {
                let table_metadata = table.metadata();

                Ok((
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
                ))
            },
            _ => Ok((None, vec![])),
        }
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
                .with_snapshot_id(table_metadata.current_snapshot_id)
                .with_sequence_number(
                    table_metadata
                        .current_snapshot(branch.as_deref())?
                        .map(|x| *x.sequence_number()),
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
