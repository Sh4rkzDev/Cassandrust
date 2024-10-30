/// A single byte that describes the possible flags for the message frame
pub enum FlagsMask {
    Compression = 0x01,
    Tracing = 0x02,
    CustomPayload = 0x04,
    Warning = 0x08,
}

pub enum ResultKind {
    Void = 0x0001,
    Rows = 0x0002,
    SetKeyspace = 0x0003,
    Prepared = 0x0004,
    SchemaChange = 0x0005,
}

pub enum ResultRowsMetadaFlagsMask {
    GlobalTablesSpec = 0x0001,
    HasMorePages = 0x0002,
    NoMetadata = 0x0004,
}
