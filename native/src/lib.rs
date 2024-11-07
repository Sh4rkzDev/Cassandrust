pub(crate) mod native_protocol;

#[cfg_attr(feature = "client", path = "client.rs")]
pub mod client;
#[cfg_attr(feature = "server", path = "server.rs")]
pub mod server;
