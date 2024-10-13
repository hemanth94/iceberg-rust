// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,utils.rs
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/// Property `iceberg.field.id` for `Column`
pub(crate) const ICEBERG_FIELD_ID: &str = "iceberg.field.id";
/// Property `iceberg.field.optional` for `Column`
pub(crate) const ICEBERG_FIELD_OPTIONAL: &str = "iceberg.field.optional";
/// Property `iceberg.field.current` for `Column`
pub(crate) const ICEBERG_FIELD_CURRENT: &str = "iceberg.field.current";

use std::collections::HashMap;

//use aws_sdk_glue::types::Column;
use iceberg_rust::spec::table_metadata::TableMetadata;

use iceberg_rust::{
    catalog::Catalog,
    spec::{
        partition::{PartitionField, PartitionSpecBuilder, Transform},
        schema::Schema,
        types::{ListType, MapType, PrimitiveType, StructField, StructType, Type},
    },
};

use aws_sdk_glue::types::Column;

type GlueSchema = Vec<Column>;

#[derive(Debug, Default)]
pub(crate) struct GlueSchemaBuilder {
    schema: GlueSchema,
    is_current: bool,
    depth: usize,
}

impl GlueSchemaBuilder {
    pub fn new(is_current: bool) -> Self {
        Self {
            schema: Vec::new(),
            is_current,
            depth: 0,
        }
    }

    pub fn build(mut self) -> GlueSchema {
        self.schema
    }

    // The recursive visit_type function
    pub fn visit_type(&mut self, r#type: &Type) -> Result<String, String> {
        match r#type {
            Type::Primitive(p) => self.primitive(p),
            Type::List(list) => {
                let element_value = self.visit_type(&list.element)?;
                self.list(list, element_value)
            }
            Type::Map(map) => {
                let key_value = self.visit_type(&map.key)?;
                let value_value = self.visit_type(&map.value)?;
                self.map(map, key_value, value_value)
            }
            Type::Struct(s) => self.visit_struct(&s),
        }
    }

    fn visit_struct(&mut self, fields: &StructType) -> Result<String, String> {
        let mut results = Vec::new();
        for field in &fields.fields {
            self.before_struct_field()?;
            let field_value = self.visit_type(&field.field_type)?;
            self.after_struct_field()?;

            // Pass a reference to field_value to self.field
            let result = self.field(field, field_value.clone())?;

            // Push a clone of field_value to results
            results.push(field_value);
        }
        Ok(format!("struct<{}>", results.join(", ")))
    }

    pub fn visit_schema(&mut self, schema: Schema) {
        self.visit_struct(&schema);
    }

    fn before_list_element(&mut self) -> Result<(), String> {
        Ok(())
    }
    fn after_list_element(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn before_struct_field(&mut self) -> Result<(), String> {
        self.depth += 1;
        Ok(())
    }

    fn after_struct_field(&mut self) -> Result<(), String> {
        self.depth -= 1;
        Ok(())
    }

    fn primitive(&mut self, p: &PrimitiveType) -> Result<String, String> {
        let glue_type = match p {
            PrimitiveType::Boolean => "boolean".to_string(),
            PrimitiveType::Int => "int".to_string(),
            PrimitiveType::Long => "bigint".to_string(),
            PrimitiveType::Float => "float".to_string(),
            PrimitiveType::Double => "double".to_string(),
            PrimitiveType::Decimal { precision, scale } => {
                format!("decimal({},{})", precision, scale)
            }
            PrimitiveType::Date => "date".to_string(),
            PrimitiveType::Time => "time".to_string(),
            PrimitiveType::Timestamp => "timestamp".to_string(),
            PrimitiveType::Timestamptz => "timestamptz".to_string(),
            PrimitiveType::String => "string".to_string(),
            PrimitiveType::Uuid => "uuid".to_string(),
            PrimitiveType::Binary => "binary".to_string(),
            PrimitiveType::Fixed(_) => "fixed".to_string(),
        };
        Ok(glue_type)
    }

    fn list(&mut self, _list: &ListType, value: String) -> Result<String, String> {
        Ok(format!("array<{}>", value))
    }

    fn map(&mut self, _map: &MapType, key_value: String, value: String) -> Result<String, String> {
        Ok(format!("map<{},{}>", key_value, value))
    }

    fn is_inside_struct(&self) -> bool {
        self.depth > 0
    }

    fn field(&mut self, field: &StructField, value: String) -> Result<String, String> {
        if self.is_inside_struct() {
            return Ok(format!("{}:{}", field.name, &value));
        }

        let parameters = HashMap::from([
            (ICEBERG_FIELD_ID.to_string(), format!("{}", field.id)),
            (
                ICEBERG_FIELD_OPTIONAL.to_string(),
                format!("{}", field.required).to_lowercase(),
            ),
            (
                ICEBERG_FIELD_CURRENT.to_string(),
                format!("{}", self.is_current).to_lowercase(),
            ),
        ]);

        let builder = Column::builder()
            .name(field.name.clone())
            .r#type(&value)
            .set_parameters(Some(parameters));

        // Handle the Result here
        match builder.build() {
            Ok(column) => {
                self.schema.push(column); // Push the successfully built Column
                Ok("".to_string()) // Return Result as expected
            }
            Err(e) => Err(format!("Failed to build column: {:?}", e)), // Return the error message
        }
    }
}

#[cfg(test)]
mod test {
    use super::*; // Import everything from the outer module
    use iceberg_rust::spec::schema::Schema;
    use iceberg_rust::spec::types::{PrimitiveType, StructField, StructType, Type};
    #[test]
    fn check_schem() {
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

        let mut builder = GlueSchemaBuilder::new(true);
        builder.visit_schema(schema);

        let glue_schema = builder.build();

        // Print the glue_schema
        for column in &glue_schema {
            println!("{:?}", column);
        }
    }
}
