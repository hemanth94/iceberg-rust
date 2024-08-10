pub struct IcebergLogicalExtensionCodec {}

impl LogicalExtensionCodec for IcebergLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &SessionContext,
    ) -> Result<Extension> {
        not_impl_err!("LogicalExtensionCodec is not provided")
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
        not_impl_err!("LogicalExtensionCodec is not provided")
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: Arc<datafusion::arrow::datatypes::Schema>,
        _ctx: &SessionContext,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        unimplemented!("try_decode_table_provider is not implemented yet")
    }

    fn try_encode_table_provider(
        &self,
        _node: Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        unimplemented!("try_encode_table_provider is not implemented yet")
    }
}