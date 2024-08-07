use std::sync::Arc;
use url::Url;
use datafusion::error::DataFusionError;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use aws_sdk_s3::Client;
use object_store::local::LocalFileSystem;

pub fn get_object_store(
    url: &str,
    region: Option<&str>,
) -> Result<Arc<dyn ObjectStore>, DataFusionError> {
    let region = region.unwrap_or("us-east-1");

    if url.starts_with("s3://") {
        let mut url = Url::parse(url).unwrap();
        if let Some(bucket_name) = url.host_str() {
            let store = Arc::new(
                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
                    .with_region(region)
                    .build()?,
            );
            return Ok(store);
        }
    } else if url.starts_with("oss://") {
        let mut url = Url::parse(url).unwrap();
        if let Some(bucket_name) = url.host_str() {
            let store = Arc::new(
                AmazonS3Builder::from_env()
                    .with_virtual_hosted_style_request(true)
                    .with_bucket_name(bucket_name)
                    .build()?,
            );
            return Ok(store);
        }
    } else {
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(url).unwrap());

        return Ok(store);

    }

    Err(DataFusionError::Execution(format!(
        "No object store available for: {url}"
    )))
}
