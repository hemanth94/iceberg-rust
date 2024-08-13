use datafusion::logical_expr::Extension;
use datafusion::logical_expr::LogicalPlan;

use datafusion::prelude::SessionContext;

use datafusion_proto::logical_plan::LogicalExtensionCodec;

use iceberg_rust::catalog::Catalog;
use iceberg_sql_catalog::SqlCatalog;
use object_store::ObjectStore;

use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::DataFusionTable;
use datafusion_common::DataFusionError;
use prost::Message;

use iceberg_rust::catalog::tabular::Tabular;

use tokio::sync::RwLock;

use iceberg_rust::catalog::identifier::Identifier;

use datafusion::datasource::TableProvider;

#[derive(Clone, PartialEq, Eq, ::prost::Message)]

pub struct TableProto {
    /// URL of the table root
    #[prost(string, tag = "1")]
    pub identifier: String,
    /// Qualified table name
    #[prost(string, tag = "2")]
    pub location: String,
    #[prost(string, tag = "3")]
    pub region: String,
    #[prost(string, tag = "4")]
    pub url: String,
}

lazy_static::lazy_static! {
    static ref RT : Runtime = tokio::runtime::Runtime::new().expect("Failed to create runtime");
}

#[derive(Debug)]
pub struct IcebergExtensionCodec {}

impl LogicalExtensionCodec for IcebergExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &SessionContext,
    ) -> datafusion::error::Result<Extension> {
        todo!()
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> datafusion::error::Result<()> {
        todo!()
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: Arc<datafusion::arrow::datatypes::Schema>,
        ctx: &SessionContext,
    ) -> datafusion::error::Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let msg = TableProto::decode(buf)
            .map_err(|_| DataFusionError::Internal("Error decoding test table".to_string()))?;

        let catalog: Arc<dyn Catalog> = tokio::task::block_in_place(|| {
            // Block on the async read call
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                Arc::new(
                    SqlCatalog::new(&msg.url, "test", &msg.location, Some(&msg.region))
                        .await
                        .unwrap(),
                )
            })
        });

        let table = tokio::task::block_in_place(|| {
            // Block on the async read call
            let identifier = Identifier::parse(&msg.identifier).unwrap();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async { catalog.load_tabular(&identifier).await })
        });

        let table = match table {
            Ok(tabular) => tabular,
            Err(e) => {
                eprintln!("Error occurred: {}", e);
                return Err(DataFusionError::External(Box::new(e)));
            }
        };

        let table_provider = DataFusionTable::from(table);

        let table_provider_arc: Arc<dyn TableProvider> = Arc::new(table_provider);

        Ok(table_provider_arc)
    }

    fn try_encode_table_provider(
        &self,
        node: Arc<dyn datafusion::datasource::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        let table = node.as_any().downcast_ref::<Arc<RwLock<Tabular>>>();

        let read_lock = tokio::task::block_in_place(|| {
            // Block on the async read call
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(table.expect("REASON").read())
        });

        let identifier = read_lock.identifier();

        let binding = read_lock.catalog();
        let location = binding.location();
        let region = binding.region();
        let url = binding.database_url();

        let msg = TableProto {
            identifier: (&identifier).to_string(), // Added `.to_string()` to convert to String type
            location: location,                    // Added `.to_string()` to convert to String type
            region: region,
            url: url,
        };
        msg.encode(buf)
            .map_err(|_| DataFusionError::Internal("Error encoding test table".to_string()))
    }
}
