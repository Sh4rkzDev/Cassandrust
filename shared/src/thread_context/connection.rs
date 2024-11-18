use std::cell::RefCell;
use std::path::PathBuf;

use crate::io_error;

struct ConnectionCtx {
    keyspace: PathBuf,
    startup: bool,
}

thread_local! {
    static CONNECTION_CTX: RefCell<ConnectionCtx> = RefCell::new(ConnectionCtx {
        keyspace: PathBuf::new(),
        startup: false,
    })
}

pub fn set_keyspace(keyspace: PathBuf) {
    CONNECTION_CTX.with(|ctx| {
        ctx.borrow_mut().keyspace = keyspace;
    });
}

pub fn get_keyspace() -> PathBuf {
    CONNECTION_CTX.with(|ctx| ctx.borrow().keyspace.clone())
}

pub fn get_keyspace_name() -> std::io::Result<String> {
    CONNECTION_CTX.with(|ctx| {
        Ok(ctx
            .borrow()
            .keyspace
            .file_name()
            .ok_or(io_error!("Error while trying to read keyspace name"))?
            .to_str()
            .ok_or(io_error!(
                "Error while trying to convert to str the keyspace name"
            ))?
            .to_string())
    })
}

pub fn set_startup(startup: bool) {
    CONNECTION_CTX.with(|ctx| {
        ctx.borrow_mut().startup = startup;
    });
}

pub fn is_startup() -> bool {
    CONNECTION_CTX.with(|ctx| ctx.borrow().startup)
}
