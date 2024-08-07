use datafusion::prelude::SessionContext;
use datafusion_iceberg::DataFusionTable;
use iceberg_rust::{
    catalog::Catalog,
    spec::{
        partition::{PartitionField, PartitionSpecBuilder, Transform},
        schema::Schema,
        types::{PrimitiveType, StructField, StructType, Type},
    },
    table::table_builder::TableBuilder,
};
use iceberg_sql_catalog::SqlCatalog;

use std::sync::Arc;

#[tokio::main]
pub(crate) async fn main() {
    let catalog: Arc<dyn Catalog> = Arc::new(
        SqlCatalog::new("sqlite://", "test", "InMemory", None)
            .await
            .unwrap(),
    );

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(
            StructType::builder()
                .with_struct_field(StructField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: Type::Primitive(PrimitiveType::Long),
                    doc: None,
                })
                .with_struct_field(StructField {
                    id: 2,
                    name: "customer_id".to_string(),
                    required: true,
                    field_type: Type::Primitive(PrimitiveType::Long),
                    doc: None,
                })
                .with_struct_field(StructField {
                    id: 3,
                    name: "product_id".to_string(),
                    required: true,
                    field_type: Type::Primitive(PrimitiveType::Long),
                    doc: None,
                })
                .with_struct_field(StructField {
                    id: 4,
                    name: "date".to_string(),
                    required: true,
                    field_type: Type::Primitive(PrimitiveType::Date),
                    doc: None,
                })
                .with_struct_field(StructField {
                    id: 5,
                    name: "amount".to_string(),
                    required: true,
                    field_type: Type::Primitive(PrimitiveType::Int),
                    doc: None,
                })
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    let partition_spec = PartitionSpecBuilder::default()
        .with_spec_id(1)
        .with_partition_field(PartitionField::new(4, 1000, "day", Transform::Day))
        .build()
        .expect("Failed to create partition spec");

    let mut builder =
        TableBuilder::new("test.orders", catalog).expect("Failed to create table builder");

    builder
        .location("/test/orders")
        .with_schema((1, schema))
        .current_schema_id(1)
        .with_partition_spec((1, partition_spec))
        .default_spec_id(1);

    let table = Arc::new(DataFusionTable::from(
        builder.build().await.expect("Failed to create table."),
    ));

    let ctx = SessionContext::new();

    ctx.register_table("orders", table).unwrap();

    ctx.sql(
        "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
        (1, 1, 1, '2020-01-01', 1),
        (2, 2, 1, '2020-01-01', 1),
        (3, 1, 1, '2020-01-01', 3),
        (4, 1, 2, '2020-02-02', 1),
        (5, 1, 1, '2020-02-02', 2),
        (6, 3, 3, '2020-02-02', 3);",
    )
    .await
    .expect("Failed to create query plan for insert")
    .collect()
    .await
    .expect("");

    ctx.sql(
        "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
        (7, 3, 2, '2020-03-03', 1),
        (8, 3, 1, '2020-03-03', 2),
        (9, 2, 3, '2020-03-03', 3);",
    )
    .await
    .expect("Failed to create query plan for insert")
    .collect()
    .await
    .expect("");

    println!("SHOW FULL TABLE");
    ctx.sql("SELECT * from orders;")
    .await
    .expect("Failed to create SELECT Query Plan")
    .show()
    .await
    .expect("Failed to execute SELECT execution plan");

    // FILTER QUERY SAMPLE
    // ctx.sql("
    // SELECT * FROM orders
    // WHERE \
    //     customer_id=10
    // ")
    // .await
    // .expect("Failed to create query plan for update")
    // .show()
    // .await
    // .expect("Failed to execute Query Execution Plan");


    // DELETE Query Sample
    println!("DELETE QUERY");
    ctx.sql("
    DELETE FROM orders
    WHERE \
        customer_id=1 OR customer_id=2 OR customer_id=3;
    ")
    .await
    .expect("Failed to create query plan for update")
    .show()
    .await
    .expect("Failed to execute Query Execution Plan");

    // DELETE Query Sample
    println!("DELETE QUERY");
    ctx.sql("
    DELETE FROM orders
    WHERE \
        customer_id=4;
    ")
    .await
    .expect("Failed to create query plan for update")
    .show()
    .await
    .expect("Failed to execute Query Execution Plan");

    println!("UPDATE QUERY");
    ctx.sql("
    UPDATE orders \
    SET product_id=10;
    ")
    .await
    .expect("Failed to create query plan for update")
    .show().await.expect("Failed to execute Query Execution Plan");

    
    ctx.sql(
        "INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES 
        (10, 3, 2, '2020-03-03', 1),
        (11, 3, 1, '2020-03-03', 2),
        (12, 2, 3, '2020-03-03', 3);",
    )
    .await
    .expect("Failed to create query plan for insert")
    .collect()
    .await
    .expect("");


    println!("SHOW FULL TABLE");
    ctx.sql("SELECT * from orders;")
    .await
    .expect("Failed to create SELECT Query Plan")
    .show()
    .await
    .expect("Failed to execute SELECT execution plan");

    // eprintln!("Updating Table");
    // UPDATE Query Sample
    println!("UPDATE QUERY");
    ctx.sql("
    UPDATE orders \
    SET product_id=10 where customer_id=2;
    ")
    .await
    .expect("Failed to create query plan for update")
    .show().await.expect("Failed to execute Query Execution Plan");


    // PRINT FINAL OUTPUT
    println!("SHOW FULL TABLE");
    ctx.sql("SELECT * from orders;")
    .await
    .expect("Failed to create SELECT Query Plan")
    .show()
    .await
    .expect("Failed to execute SELECT Query Execution Plan");
}
