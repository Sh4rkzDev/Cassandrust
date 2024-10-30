mod thread_context;

#[macro_export]
macro_rules! map_io_error {
    ($msg:expr) => {
        |_: _| std::io::Error::new(std::io::ErrorKind::InvalidData, $msg)
    };
}

#[macro_export]
macro_rules! io_error {
    ($msg:expr) => {
        std::io::Error::new(std::io::ErrorKind::InvalidData, $msg)
    };
}

#[macro_export]
macro_rules! not_found_error {
    ($msg:expr) => {
        std::io::Error::new(std::io::ErrorKind::NotFound, $msg)
    };
}

pub use thread_context::connection::get_keyspace;
pub use thread_context::connection::get_keyspace_name;
pub use thread_context::connection::is_startup;
pub use thread_context::connection::set_keyspace;
pub use thread_context::connection::set_startup;
