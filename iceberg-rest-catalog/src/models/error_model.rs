/*
 * Apache Iceberg REST Catalog API
 *
 * Defines the specification for the first version of the REST Catalog API. Implementations should ideally support both Iceberg table specs v1 and v2, with priority given to v2.
 *
 * The version of the OpenAPI document: 0.0.1
 *
 * Generated by: https://openapi-generator.tech
 */

use crate::models;

/// ErrorModel : JSON error payload returned in a response with further details on the error
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct ErrorModel {
    /// Human-readable error message
    #[serde(rename = "message")]
    pub message: String,
    /// Internal type definition of the error
    #[serde(rename = "type")]
    pub r#type: String,
    /// HTTP response code
    #[serde(rename = "code")]
    pub code: i32,
    #[serde(rename = "stack", skip_serializing_if = "Option::is_none")]
    pub stack: Option<Vec<String>>,
}

impl ErrorModel {
    /// JSON error payload returned in a response with further details on the error
    pub fn new(message: String, r#type: String, code: i32) -> ErrorModel {
        ErrorModel {
            message,
            r#type,
            code,
            stack: None,
        }
    }
}
