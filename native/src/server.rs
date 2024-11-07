use std::io::Read;

use crate::native_protocol::native::Frame;

pub fn read_request<R: Read>(stream: &mut R) -> std::io::Result<Frame> {
    Frame::read(stream)
}
