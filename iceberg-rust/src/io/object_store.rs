use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use std::sync::Arc;
use url::Url;
use std::{fs};
use dotenv::dotenv;


fn get_aws_credentials() -> Result<Option<(String, String)>, String> {
    dotenv().ok(); // Load environment variables from .env file if present

    // Check if running on EC2
    fn is_ec2() -> bool {
        let path = "/sys/devices/virtual/dmi/id/product_uuid";
        match fs::read_to_string(path) {
            Ok(content) => content.starts_with("ec2"),
            Err(_) => false,
        }
    }

    return Ok(None);
    /*

    // Check if running on ECS
    fn is_ecs() -> bool {
        env::var("ECS_CONTAINER_METADATA_URI").is_ok()
    }

    // If credentials are passed explicitly
    if let (Ok(access_key), Ok(secret_key)) = (env::var("AWS_ACCESS_KEY_ID"), env::var("AWS_SECRET_ACCESS_KEY")) {
        return Ok(Some((access_key, secret_key)));
    }

    // If running on EC2 or ECS, credentials should be handled by environment variables or IAM roles
    if is_ec2() || is_ecs() {
        println!("Using instance IAM role or environment variables for credentials.");
        return Ok(None); // No credentials to return, SDK will use default method
    }

    // Otherwise, read from the config file (local setup)
    let config_file_path = dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")).join(".aws/credentials");
    if config_file_path.exists() {
        let contents = fs::read_to_string(config_file_path).expect("Unable to read config file");
        let mut in_default_profile = false;
        let mut access_key = None;
        let mut secret_key = None;

        for line in contents.lines() {
            if line.starts_with('[') && line.ends_with(']') {
                if line == "[default]" {
                    in_default_profile = true;
                } else {
                    in_default_profile = false;
                }
            } else if in_default_profile {
                if line.starts_with("aws_access_key_id") {
                    access_key = Some(line.split('=').nth(1).unwrap().trim().to_string());
                } else if line.starts_with("aws_secret_access_key") {
                    secret_key = Some(line.split('=').nth(1).unwrap().trim().to_string());
                }
            }
        }

        if let (Some(key), Some(secret)) = (access_key, secret_key) {
            return Ok(Some((key, secret)));
        }
        println!("AWS credentials not found in the default profile.");
    }

    Err("No valid AWS credentials found in environment or config file.".to_string())

     */
}

pub fn get_object_store(url: &str, region: Option<&str>) -> Arc<dyn ObjectStore> {
    let region = region.unwrap_or("us-east-1");

    if url.starts_with("s3://") {
        let url = Url::parse(url).expect("Failed to parse S3 URL");

        if let Some(bucket_name) = url.host_str() {
            let object_store: Arc<dyn ObjectStore> = match get_aws_credentials() {
                Ok(Some((access_key, secret_key))) => {
                    Arc::new(
                        AmazonS3Builder::new()
                            .with_region(region)
                            .with_bucket_name(bucket_name)
                            .with_access_key_id(access_key)
                            .with_secret_access_key(secret_key)
                            .build()
                            .expect("Failed to build S3 object store"),
                    )
                }
                Ok(None) => {
                    // If no explicit credentials, use environment variables or default method
                    Arc::new(
                        AmazonS3Builder::from_env()
                            .with_bucket_name(bucket_name)
                            .with_region(region)
                            .build()
                            .expect("Failed to build S3 object store from env"),
                    )
                }
                Err(e) => panic!("Failed to load credentials: {:?}", e),
            };

            return object_store;
        }

    } else if url.starts_with("oss://") {
        let url = Url::parse(url).expect("Failed to parse OSS URL");
        if let Some(bucket_name) = url.host_str() {
            let object_store: Arc<dyn ObjectStore>  = Arc::new(
                AmazonS3Builder::from_env()
                    .with_virtual_hosted_style_request(true)
                    .with_bucket_name(bucket_name)
                    .build()
                    .expect("Failed to build OSS object store"),
            );

            return object_store;
        }
    }

    panic!("No object store available for: {}", url);
}