mod schema;

mod utils;

use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_glue::config::ProvideCredentials;
use aws_sdk_sts::Client;
use aws_sdk_glue;
use aws_types::region::Region;
use derive_builder::Builder;
use object_store;
use std::any::Any;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use uuid::Uuid;

use crate::schema::GlueSchemaBuilder;
use crate::utils::{
    convert_to_database, get_metadata_location, EXTERNAL_TABLE, ICEBERG, METADATA_LOCATION,
    PREV_METADATA_LOCATION, TABLE_TYPE,
};

use iceberg_rust::{
    catalog::{
        bucket::{parse_bucket, Bucket},
        commit::{apply_table_updates, check_table_requirements, CommitTable, CommitView},
        identifier::Identifier,
        namespace::Namespace,
        tabular::Tabular,
        Catalog,
    },
    error::Error as IcebergError,
    io::object_store::get_object_store,
    materialized_view::MaterializedView,
    spec::{
        materialized_view_metadata::MaterializedViewMetadata, table_metadata::TableMetadata,
        tabular::TabularMetadata, view_metadata::ViewMetadata,
    },
    table::Table,
    view::View,
};

use iceberg_rust::util::strip_prefix;

use aws_sdk_glue::types::{StorageDescriptor, TableInput};
use dashmap::DashMap;
use futures::lock::Mutex;

use iceberg_rust::catalog::CatalogList;
use object_store::ObjectStore;

#[derive(Builder, Debug, Clone)]
pub struct GlueCatalogConfig {
    #[builder(default, setter(strip_option))]
    uri: Option<String>,
    #[builder(default, setter(strip_option))]
    catalog_id: Option<String>,
    warehouse: String,
    #[builder(default)]
    props: HashMap<String, String>,
}

#[derive(Debug, Clone)]
struct GlueClient(aws_sdk_glue::Client);

#[derive(Debug)]
pub struct GlueCatalog {
    config: GlueCatalogConfig,
    client: GlueClient,
    object_store: Arc<dyn ObjectStore>,
}

impl GlueCatalog {
    pub async fn new(glueconfig: GlueCatalogConfig) -> Result<Self, Box<dyn Error>> {
        let region = glueconfig
            .props
            .get("region")
            .unwrap_or(&"us-east-1".to_string())
            .to_string();
        let profile_name = glueconfig
            .props
            .get("profile_name")
            .unwrap_or(&"default".to_string())
            .to_string();

        let config = aws_config::from_env()
            .profile_name(profile_name) // Set the profile here
            .region(Region::new(region.clone()))
            .load()
            .await;

        let object_store = get_object_store(&glueconfig.warehouse, Some(&region.clone()));

        let client = aws_sdk_glue::Client::new(&config);

        let credentials_provider = config.credentials_provider();

        if let Some(provider) = credentials_provider {
            let creds = provider.provide_credentials().await?;
            println!("Access Key: {}", creds.access_key_id());
            println!("Secret Key: {}", creds.secret_access_key());
        } else {
            println!("No credentials provider available");
        }

        Ok(GlueCatalog {
            config: glueconfig,
            client: GlueClient(client),
            object_store: object_store,
        })
    }
}

#[async_trait]
impl Catalog for GlueCatalog {
    async fn create_namespace(
        &self,
        namespace: &Namespace,
        properties: Option<HashMap<String, String>>,
    ) -> Result<HashMap<String, String>, IcebergError> {
        //let db_input = convert_to_database(namespace, properties);
        todo!()
    }

    async fn drop_namespace(&self, namespace: &Namespace) -> Result<(), IcebergError> {
        todo!()
    }

    async fn load_namespace(
        &self,
        namespace: &Namespace,
    ) -> Result<HashMap<String, String>, IcebergError> {
        todo!()
    }

    async fn update_namespace(
        &self,
        namespace: &Namespace,
        updates: Option<HashMap<String, String>>,
        removals: Option<Vec<String>>,
    ) -> Result<(), IcebergError> {
        todo!()
    }

    async fn namespace_exists(&self, namespace: &Namespace) -> Result<bool, IcebergError> {
        todo!()
    }

    async fn list_tabulars(&self, namespace: &Namespace) -> Result<Vec<Identifier>, IcebergError> {
        let database = namespace.to_string();

        let mut table_list: Vec<Identifier> = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let builder = match &next_token {
                Some(token) => self
                    .client
                    .0
                    .get_tables()
                    .database_name(&database)
                    .next_token(token),
                None => self.client.0.get_tables().database_name(&database),
            };
            //let builder = with_catalog_id!(builder, self.config);
            let resp = builder
                .send()
                .await
                .map_err(|_| IcebergError::InvalidFormat(format!("Error ")))?;

            println!("resp {:?}", resp);

            let tables: Vec<Identifier> = resp
                .table_list()
                .iter()
                .map(|tbl| Identifier::try_new(&[database.clone(), tbl.name().to_string()], None)) // Result<Identifier, Error>
                .collect::<Result<Vec<Identifier>, iceberg_rust::error::Error>>()?; // Collect into Result<Vec<Identifier>, Error>

            table_list.extend(tables);

            next_token = resp.next_token().map(ToOwned::to_owned);
            if next_token.is_none() {
                break;
            }
        }

        Ok(table_list)
    }

    async fn list_namespaces(&self, parent: Option<&str>) -> Result<Vec<Namespace>, IcebergError> {
        let mut database_list: Vec<Namespace> = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let builder = match &next_token {
                Some(token) => self.client.0.get_databases().next_token(token),
                None => self.client.0.get_databases(),
            };

            let resp = builder
                .send()
                .await
                .map_err(|_| IcebergError::InvalidFormat(format!("Error ")))?;

            let dbs: Vec<Namespace> = resp
                .database_list()
                .iter()
                .map(|db| Namespace::try_new(&[db.name().to_string()]).unwrap())
                .collect();

            database_list.extend(dbs);

            next_token = resp.next_token().map(ToOwned::to_owned);

            if next_token.is_none() {
                break Ok(database_list);
            }
        }
    }

    async fn tabular_exists(&self, identifier: &Identifier) -> Result<bool, IcebergError> {
        todo!()
    }

    async fn drop_table(&self, identifier: &Identifier) -> Result<(), IcebergError> {
        todo!()
    }

    async fn drop_view(&self, identifier: &Identifier) -> Result<(), IcebergError> {
        todo!()
    }

    async fn drop_materialized_view(&self, identifier: &Identifier) -> Result<(), IcebergError> {
        todo!()
    }

    async fn load_tabular(
        self: Arc<Self>,
        identifier: &Identifier,
    ) -> Result<Tabular, IcebergError> {
        let database = identifier.namespace().to_string();
        let tablename = identifier.name().to_string();

        let builder = self
            .client
            .0
            .get_table()
            .database_name(database.clone())
            .name(tablename.clone());

        let glue_table_output = builder
            .send()
            .await
            .map_err(|_| IcebergError::InvalidFormat("Failed to build table input".to_string()))?;

        match glue_table_output.table() {
            None => Err(IcebergError::InvalidFormat(format!(
                "Table object for database: {} and table: {} does not exist",
                database, tablename
            ))),
            Some(table) => {
                let metadata_location = get_metadata_location(&table.parameters).map_err(|e| {
                    IcebergError::InvalidFormat(format!("Error getting metadata location: {}", e))
                })?;

                let bytes = &self
                    .object_store
                    .get(&strip_prefix(&metadata_location).as_str().into())
                    .await?
                    .bytes()
                    .await?;

                let metadata: TabularMetadata = serde_json::from_str(std::str::from_utf8(bytes)?)?;

                match metadata {
                    TabularMetadata::Table(metadata) => Ok(Tabular::Table(
                        Table::new(identifier.clone(), self.clone(), metadata).await?,
                    )),
                    TabularMetadata::View(metadata) => Ok(Tabular::View(
                        View::new(identifier.clone(), self.clone(), metadata).await?,
                    )),
                    TabularMetadata::MaterializedView(metadata) => Ok(Tabular::MaterializedView(
                        MaterializedView::new(identifier.clone(), self.clone(), metadata).await?,
                    )),
                }
            }
        }
    }

    async fn create_table(
        self: Arc<Self>,
        identifier: Identifier,
        metadata: TableMetadata,
    ) -> Result<iceberg_rust::table::Table, IcebergError> {
        /*
        TO-DO
        --> Branch
         */
        let schema = metadata.current_schema(None);
        let bucket = parse_bucket(&self.config.warehouse)?;

        let location = metadata.location.to_string();

        let table_location = format!("{}{}", bucket, location);
        let database = identifier.namespace().to_string();
        let tablename = identifier.name().to_string();

        let uuid = Uuid::new_v4();
        let version = &metadata.last_sequence_number;
        let metadata_json = serde_json::to_string(&metadata)?;
        let metadata_location = location.clone()
            + "/metadata/"
            + &version.to_string()
            + "-"
            + &uuid.to_string()
            + ".metadata.json";

        let bucket_metadata_location = format!("{}{}", bucket, metadata_location);

        self.object_store
            .put(
                &strip_prefix(&metadata_location).into(),
                metadata_json.into(),
            )
            .await?;

        // Create a Glue schema using the builder
        let mut builder = GlueSchemaBuilder::new(true);
        builder.visit_schema(schema?.clone());
        let glue_schema = builder.build();

        // Create the storage descriptor with the schema and metadata location
        let storage_descriptor = StorageDescriptor::builder()
            .set_columns(Some(glue_schema))
            .location(table_location.clone())
            .build();

        // Set up parameters for the table
        let mut parameters = HashMap::new();
        parameters.insert(TABLE_TYPE.to_string(), ICEBERG.to_string());
        parameters.insert(
            METADATA_LOCATION.to_string(),
            bucket_metadata_location.to_string(),
        );

        // Create the table input using the builder
        let table_input_builder = TableInput::builder()
            .name(tablename)
            .set_parameters(Some(parameters))
            .storage_descriptor(storage_descriptor)
            .table_type(EXTERNAL_TABLE);

        // Build the table input and handle errors
        let table_input = table_input_builder.build().map_err(|_| {
            IcebergError::InvalidFormat("Table update on an entity that is not a table".to_owned())
        })?;

        // Send the create table request to AWS Glue
        let create_response = self
            .client
            .0
            .create_table()
            .database_name(database)
            .table_input(table_input)
            .send()
            .await;

        // Handle the response from AWS Glue
        match create_response {
            Ok(response) => {
                println!("Table created successfully: {:?}", response);
                self.clone()
                    .load_tabular(&identifier)
                    .await
                    .and_then(|x| match x {
                        Tabular::Table(table) => Ok(table),
                        _ => Err(IcebergError::InvalidFormat(
                            "Table update on an entity that is nor a table".to_owned(),
                        )),
                    })
            }
            Err(e) => {
                eprintln!("Failed to create table: {:?}", e);
                let iceberg_error: IcebergError =
                    IcebergError::InvalidFormat(format!("Failed to create table: {}", e));
                Err(iceberg_error)
            }
        }
    }

    async fn create_view(
        self: Arc<Self>,
        identifier: Identifier,
        metadata: ViewMetadata,
    ) -> Result<View, IcebergError> {
        todo!()
    }

    async fn create_materialized_view(
        self: Arc<Self>,
        identifier: Identifier,
        metadata: MaterializedViewMetadata,
    ) -> Result<MaterializedView, IcebergError> {
        todo!()
    }

    async fn update_table(
        self: Arc<Self>,
        commit: CommitTable,
    ) -> Result<iceberg_rust::table::Table, IcebergError> {
        let identifier = commit.identifier;

        let database = identifier.namespace().to_string();
        let tablename = identifier.name().to_string();
        let bucket = parse_bucket(&self.config.warehouse)?;

        let builder = self
            .client
            .0
            .get_table()
            .database_name(database.clone())
            .name(tablename.clone());

        let glue_table_output = builder
            .send()
            .await
            .map_err(|_| IcebergError::InvalidFormat("Failed to build table input".to_string()))?;

        match glue_table_output.table() {
            None => Err(IcebergError::InvalidFormat(format!(
                "Table object for database: {} and table: {} does not exist",
                database, tablename
            ))),
            Some(table) => {
                let previous_metadata_location =
                    get_metadata_location(&table.parameters).map_err(|e| {
                        IcebergError::InvalidFormat(format!(
                            "Error getting metadata location: {}",
                            e
                        ))
                    })?;

                let bytes = &self
                    .object_store
                    .get(&strip_prefix(&previous_metadata_location).as_str().into())
                    .await?
                    .bytes()
                    .await?;

                let metadata: TabularMetadata = serde_json::from_str(std::str::from_utf8(bytes)?)?;

                let mut table_metadata = match metadata {
                    TabularMetadata::Table(metadata) => metadata,
                    _ => {
                        return Err(IcebergError::InvalidFormat(
                            "Expected table metadata, but found something else".to_owned(),
                        ));
                    }
                };

                // Now you can use `table_metadata` for `check_table_requirements`
                if !check_table_requirements(&commit.requirements, &table_metadata) {
                    return Err(IcebergError::InvalidFormat(
                        "Table requirements not valid".to_owned(),
                    ));
                }

                apply_table_updates(&mut table_metadata, commit.updates)?;

                let transaction_uuid = Uuid::new_v4();

                let version = table_metadata.last_sequence_number;

                let metadata_location = table_metadata.location.clone()
                    + "/metadata/"
                    + &version.to_string()
                    + "-"
                    + &transaction_uuid.to_string()
                    + ".metadata.json";

                let bucket_metadata_location = format!("{}{}", bucket, metadata_location);

                self.object_store
                    .put(
                        &strip_prefix(&metadata_location).into(),
                        serde_json::to_string(&table_metadata)?.into(),
                    )
                    .await?;

                let table_location = format!("{}{}", bucket, table_metadata.location.clone());
                let schema = table_metadata.current_schema(None);

                let mut builder = GlueSchemaBuilder::new(true);
                builder.visit_schema(schema?.clone());
                let glue_schema = builder.build();

                // Create the storage descriptor with the schema and metadata location

                let storage_descriptor = StorageDescriptor::builder()
                    .set_columns(Some(glue_schema))
                    .location(table_location.clone())
                    .build();

                // Set up parameters for the table
                let mut parameters = HashMap::new();
                parameters.insert(TABLE_TYPE.to_string(), ICEBERG.to_string());
                parameters.insert(
                    METADATA_LOCATION.to_string(),
                    bucket_metadata_location.to_string(),
                );
                parameters.insert(
                    PREV_METADATA_LOCATION.to_string(),
                    previous_metadata_location.to_string(),
                );

                // Create the table input using the builder
                let table_input_builder = TableInput::builder()
                    .name(tablename)
                    .set_parameters(Some(parameters))
                    .storage_descriptor(storage_descriptor)
                    .table_type(EXTERNAL_TABLE);

                let table_input = table_input_builder.build().map_err(|_| {
                    IcebergError::InvalidFormat(
                        "Table update on an entity that is not a table".to_owned(),
                    )
                })?;

                // Send the update table request to AWS Glue
                let update_response = self
                    .client
                    .0
                    .update_table()
                    .database_name(database)
                    .table_input(table_input)
                    .send()
                    .await;

                match update_response {
                    Ok(response) => {
                        return self.clone().load_tabular(&identifier).await.and_then(
                            |x| match x {
                                Tabular::Table(table) => Ok(table),
                                _ => Err(IcebergError::InvalidFormat(
                                    "Table update on an entity that is not a table".to_owned(),
                                )),
                            },
                        );
                    }
                    Err(e) => {
                        return Err(IcebergError::InvalidFormat(format!(
                            "Failed to update table: {}",
                            e
                        )));
                    }
                }
            }
        }
    }

    async fn update_view(self: Arc<Self>, commit: CommitView) -> Result<View, IcebergError> {
        todo!()
    }

    async fn update_materialized_view(
        self: Arc<Self>,
        commit: CommitView,
    ) -> Result<MaterializedView, IcebergError> {
        todo!()
    }

    fn object_store(&self, bucket: Bucket) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    fn database_url(&self) -> String {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn location(&self) -> String {
        self.config.warehouse.clone()
    }

    fn region(&self) -> String {
        self.config
            .props
            .get("region")
            .unwrap_or(&"us-east-1".to_string())
            .to_string()
    }
}

#[derive(Debug)]
pub struct glueCatalogList {
    config: GlueCatalogConfig,
    client: GlueClient,
    object_store: Arc<dyn ObjectStore>,
}

#[async_trait]
impl CatalogList for GlueCatalog {
    async fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>> {
        Some(Arc::new(GlueCatalog {
            config: self.config.clone(),
            client: self.client.clone(),
            object_store: self.object_store.clone(),
        }))
    }
    async fn list_catalogs(&self) -> Vec<String> {
        let region_provider = RegionProviderChain::default_provider().or_else("us-west-2"); // Replace with your AWS region
        let config = aws_config::from_env().region(region_provider).load().await;
        let client = Client::new(&config);

        let response = client.get_caller_identity().send().await.unwrap();
        let mut account_number = Vec::new();

        // Extract and print the account number
        if let Some(account) = response.account() {
            println!("Account ID: {}", account);
            account_number.push(account.to_string());
        } else {
            println!("Unable to retrieve account ID.");
        }
        account_number


    }
}
