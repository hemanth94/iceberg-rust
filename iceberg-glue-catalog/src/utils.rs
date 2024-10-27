use std::collections::HashMap;
use uuid::Uuid;

use iceberg_rust::{

    catalog::{

        commit::{
            TableUpdate,
        },


    },

};
use iceberg_rust::catalog::namespace::Namespace;
use iceberg_rust::spec::tabular::TabularMetadataRef;
use aws_sdk_glue::types::{DatabaseInput};



pub const EXTERNAL_TABLE: &str = "EXTERNAL_TABLE";
/// Parameter key `table_type` for `TableInput`
pub const TABLE_TYPE: &str = "table_type";
/// Parameter value `table_type` for `TableInput`
pub const ICEBERG: &str = "ICEBERG";

pub const METADATA_LOCATION: &str = "metadata_location";

pub const PREV_METADATA_LOCATION: &str = "prev_metadata_location";

pub(crate) fn get_metadata_location(
    parameters: &Option<HashMap<String, String>>,
) -> Result<String, Box<dyn Error>> {
    match parameters {
        Some(properties) => match properties.get(METADATA_LOCATION) {
            Some(location) => Ok(location.to_string()),
            None => Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("No '{}' set on table", METADATA_LOCATION),
            ))),
        },
        None => Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "No 'parameters' set on table. Location of metadata is undefined",
        ))),
    }
}

pub fn get_location_from_update(updates: &[TableUpdate]) -> Option<&String> {
    println!("get_location_from_update {:?}",updates);
    for update in updates {
        if let TableUpdate::SetLocation { location } = update {
            println!("get_location_from_update {:?}",location);
            return Some(location);
        }
    }
    None
}

pub fn new_metadata_location(metadata: TabularMetadataRef<'_>) -> String {
    let transaction_uuid = Uuid::new_v4();
    let version = metadata.sequence_number();

    metadata.location().to_string()
        + "/metadata/"
        + &version.to_string()
        + "-"
        + &transaction_uuid.to_string()
        + ".metadata.json"
}

use std::error::Error;
use aws_sdk_glue::error::BuildError;

pub(crate) fn validate_namespace(namespace: &Namespace) -> Result<String, Box<dyn Error>> {
    let name = namespace.as_ref();

    if name.len() != 1 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Invalid database name: {:?}, hierarchical namespaces are not supported",
                namespace
            ),
        )));
    }

    let name = name[0].clone();

    if name.is_empty() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid database, provided namespace is empty".to_string(),
        )));
    }

    Ok(name)
}

pub(crate) fn convert_to_database(
    namespace: &Namespace,
    properties: Option<HashMap<String, String>>,
) -> Result<DatabaseInput, Box<dyn Error>> {
    let db_name = validate_namespace(namespace).map_err(|e| {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Namespace validation failed: {}", e),
        )) as Box<dyn Error>
    })?;

    let mut builder = DatabaseInput::builder().name(db_name);

    // Use unwrap_or_default() to handle None case for properties
    for (k, v) in properties.unwrap_or_default() {
        match k.as_str() {
            DESCRIPTION => {
                builder = builder.description(v);
            }
            LOCATION => {
                builder = builder.location_uri(v);
            }
            _ => {
                builder = builder.parameters(k, v);
            }
        }
    }

    builder.build().map_err(|e| {
        Box::new(e) as Box<dyn Error> // Adjust this as necessary based on what `builder.build()` returns
    })
}
