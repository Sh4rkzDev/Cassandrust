/// A single byte that describes the possible flags for the message frame
pub enum FlagsMask {
    Compression = 0x01,
    Tracing = 0x02,
    CustomPayload = 0x04,
    Warning = 0x08,
}
