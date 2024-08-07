use std::sync::Arc;
use url::Url;
use datafusion::error::DataFusionError;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use aws_sdk_s3::Client;

pub fn get_object_store(
    url: &Url,
    region: Option<&str>,
) -> Result<Arc<dyn ObjectStore>, DataFusionError> {
    let url_str = url.as_str();
    let region = region.unwrap_or("us-east-1");

    if url_str.starts_with("s3://") {
        if let Some(bucket_name) = url.host_str() {
            let store = Arc::new(
                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
                    .with_region(region)
                    .build()?,
            );
            return Ok(store);
        }
    } else if url_str.starts_with("oss://") {
        if let Some(bucket_name) = url.host_str() {
            let store = Arc::new(
                AmazonS3Builder::from_env()
                    .with_virtual_hosted_style_request(true)
                    .with_bucket_name(bucket_name)
                    .build()?,
            );
            return Ok(store);
        }
    }

    Err(DataFusionError::Execution(format!(
        "No object store available for: {url}"
    )))
}
