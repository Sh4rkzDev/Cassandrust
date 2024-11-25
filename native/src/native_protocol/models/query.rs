use query::{process_query, Query};

use super::consistency::ConsistencyLevel;

/// A byte representing the possible flags to use within a QUERY message
#[derive(Debug)]
enum QueryFlagsMask {
    // Values = 0x01,
    SkipMetadata = 0x02,
    // PageSize = 0x04,
    // WithPagingState = 0x08,
    // WithNamesForValues = 0x40,
}

#[derive(Debug)]
pub struct QueryMsg {
    pub query_str: String,
    pub table: String,
    pub query: Query,
    pub consistency: ConsistencyLevel,
    pub flags: u8,
}

impl QueryMsg {
    pub fn new(
        query_str: String,
        consistency: ConsistencyLevel,
        flags: u8,
    ) -> std::io::Result<Self> {
        let (query, table) = process_query(query_str.as_str())?;
        Ok(QueryMsg {
            query_str,
            query,
            table,
            consistency,
            flags,
        })
    }

    /// Processes the flags of the query message
    ///
    /// # Returns
    /// - An optional string containing the query to be executed immediately. In case of `None`, the query will be executed later.
    /// - The number of bytes read from the reader

    // pub fn process_flags<R: Read>(
    //     &mut self,
    //     reader: &mut R,
    // ) -> std::io::Result<(Option<String>, u32)> {
    //     let mut query = None;
    //     let mut bytes_read = 0;
    //     if self.flags & QueryFlagsMask::Values as u8 == QueryFlagsMask::Values as u8 {
    //         let (query_opt, read) = self.process_values(reader)?;
    //         query = Some(query_opt);
    //         bytes_read += read;
    //     }

    //     Ok((query, bytes_read))
    // }

    // fn process_values<R: Read>(&mut self, reader: &mut R) -> std::io::Result<(String, u32)> {
    //     let mut updated_query_str = String::from(&self.query_str);
    //     let mut bytes_read = 0;

    //     let mut n_buffer = [0u8; 2];
    //     reader.read_exact(&mut n_buffer)?;
    //     let n = u16::from_be_bytes(n_buffer);
    //     bytes_read += 2;

    //     for _ in 0..n {
    //         let mut name_value = "?".to_string();
    //         if self.flags & QueryFlagsMask::WithNamesForValues as u8
    //             == QueryFlagsMask::WithNamesForValues as u8
    //         {
    //             let (name, read) = read_string(reader)?;
    //             name_value = ":".to_string() + name.as_str();
    //             bytes_read += read;
    //         }
    //         let (value_opt, read) = read_value(reader)?;
    //         bytes_read += read;

    //         let value = match value_opt {
    //             Some(v) => {
    //                 if v.len() != 0 {
    //                     let ctx = get_context()
    //                         .read()
    //                         .map_err(map_io_error!("The lock is poisoned"))?;
    //                     let table_schema = ctx.get_table_schema();
    //                     let tables = table_schema
    //                         .read()
    //                         .map_err(map_io_error!("The lock is poisoned"))?;
    //                     let schema = tables
    //                         .tables
    //                         .get(self.table.as_str())
    //                         .ok_or(io_error!(format!("Table '{}' is not found", self.table)))?;
    //                     // TODO get_parse_function(...)
    //                     let parser = schema.get_parse_function("").ok_or(io_error!(format!(
    //                         "Column '{}' is not found in table '{}'",
    //                         name_value, self.table
    //                     )))?;
    //                     todo!("Get what column is referencing to")
    //                 } else {
    //                     "NULL"
    //                 }
    //             }
    //             None => "",
    //         };

    //         // TODO
    //         updated_query_str = updated_query_str.replacen(name_value.as_str(), value, 1)
    //     }

    //     let mut values_buffer = [0u8; 4];
    //     reader.read_exact(&mut values_buffer)?;
    //     self.page_size = i32::from_be_bytes(values_buffer);
    //     bytes_read += 4;

    //     Ok((updated_query_str, bytes_read))
    // }

    pub fn skip_metadata(&self) -> bool {
        self.flags & QueryFlagsMask::SkipMetadata as u8 == QueryFlagsMask::SkipMetadata as u8
    }
}
