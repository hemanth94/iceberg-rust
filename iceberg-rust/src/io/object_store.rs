use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::sync::Arc;
use url::Url;

pub fn get_object_store(url: &str, region: Option<&str>) -> Arc<dyn ObjectStore> {
    println!("In the object store : {:?}", url);
    let region = region.unwrap_or("us-east-1");

    if url.starts_with("s3://") {
        let url = Url::parse(url).expect("Failed to parse S3 URL");
        if let Some(bucket_name) = url.host_str() {
            return Arc::new(
                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
                    .with_region(region)
                    .build()
                    .expect("Failed to build Amazon S3 object store"),
            );
        }
    } else if url.starts_with("oss://") {
        let url = Url::parse(url).expect("Failed to parse OSS URL");
        if let Some(bucket_name) = url.host_str() {
            return Arc::new(
                AmazonS3Builder::from_env()
                    .with_virtual_hosted_style_request(true)
                    .with_bucket_name(bucket_name)
                    .build()
                    .expect("Failed to build OSS object store"),
            );
        }
    } else if url.starts_with("InMemory") {
        return Arc::new(InMemory::new());
    } else {
        return Arc::new(
            LocalFileSystem::new_with_prefix(url)
                .expect("Failed to create local file system object store"),
        );
    }

    panic!("No object store available for: {}", url);
}
