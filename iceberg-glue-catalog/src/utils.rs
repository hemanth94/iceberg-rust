use std::collections::HashMap;
use std::error::Error;
use uuid::Uuid;

use iceberg_rust::{

    catalog::{

        commit::{
            TableUpdate,
        },


    },

};
use iceberg_rust::spec::tabular::TabularMetadataRef;


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