use shared::io_error;

use crate::native_protocol::parsers::{
    bytes::Bytes,
    string::{read_string, write_string},
};

use std::io::{Cursor, Read, Write};

#[derive(Debug, PartialEq)]
pub enum DataTypeFlags {
    Boolean = 0x0004,
    Float = 0x0008,
    Int = 0x0009,
    Varchar = 0x000D,
    Timestamp = 0x000B,
    Inet = 0x0010,
}

impl DataTypeFlags {
    pub fn new(flag: u16) -> std::io::Result<DataTypeFlags> {
        match flag {
            0x0004 => Ok(DataTypeFlags::Boolean),
            0x0008 => Ok(DataTypeFlags::Float),
            0x0009 => Ok(DataTypeFlags::Int),
            0x000B => Ok(DataTypeFlags::Timestamp),
            0x000D => Ok(DataTypeFlags::Varchar),
            0x0010 => Ok(DataTypeFlags::Inet),
            _ => Err(io_error!(format!("Invalid data flag: {flag}"))),
        }
    }
    pub fn to_be_bytes(&self) -> [u8; 2] {
        match self {
            DataTypeFlags::Boolean => [0x00, 0x04],
            DataTypeFlags::Float => [0x00, 0x08],
            DataTypeFlags::Int => [0x00, 0x09],
            DataTypeFlags::Timestamp => [0x00, 0x0B],
            DataTypeFlags::Varchar => [0x00, 0x0D],
            DataTypeFlags::Inet => [0x00, 0x10],
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: DataTypeFlags,
}

impl ColumnSpec {
    pub fn new(name: String, data_type: DataTypeFlags) -> Self {
        ColumnSpec { name, data_type }
    }

    fn read<R: Read>(reader: &mut R) -> std::io::Result<(Self, u32)> {
        let (name, read) = read_string(reader)?;
        let mut bytes_read = read;

        let mut buf_data_type = [0u8; 2];
        reader.read_exact(&mut buf_data_type)?;
        let data_type_u16 = u16::from_be_bytes(buf_data_type);
        let data_type = DataTypeFlags::new(data_type_u16)?;
        bytes_read += 2;

        Ok((ColumnSpec { name, data_type }, bytes_read))
    }

    fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<u32> {
        let written = write_string(writer, &self.name)?;

        writer.write_all(&self.data_type.to_be_bytes())?;

        Ok(written + 2)
    }
}

enum RowsMetadaFlagsMask {
    GlobalTablesSpec = 0x0001,
    HasMorePages = 0x0002,
    NoMetadata = 0x0004,
}

#[derive(Debug)]
pub struct RowMetadata {
    pub flags: i32,
    pub columns_count: i32,
    /// `keyspace` and `table` names for the global table spec
    pub global_table_spec: Option<(String, String)>,
    pub column_specs: Option<Vec<ColumnSpec>>,
}

impl RowMetadata {
    pub fn new(
        flags: i32,
        columns_count: i32,
        global_table_spec: Option<(String, String)>,
        column_specs: Option<Vec<ColumnSpec>>,
    ) -> std::io::Result<Self> {
        if flags & RowsMetadaFlagsMask::GlobalTablesSpec as i32
            == RowsMetadaFlagsMask::GlobalTablesSpec as i32
            && global_table_spec.is_none()
        {
            return Err(io_error!("Global table spec is None but set to be present"));
        }
        // 0x0002 => {
        //     if let None = paging_state {
        //         return Err("Paging state spec is None".to_string());
        //     }
        // }
        if flags & RowsMetadaFlagsMask::NoMetadata as i32 == RowsMetadaFlagsMask::NoMetadata as i32
            && global_table_spec.is_some()
        {
            return Err(io_error!("Global table spec is Some but set to be absent"));
        }
        if columns_count
            != column_specs
                .as_ref()
                .map(|cols| cols.len() as i32)
                .unwrap_or(columns_count)
        {
            return Err(io_error!(
                "Columns count does not match the number of columns"
            ));
        }
        Ok(RowMetadata {
            flags,
            columns_count,
            global_table_spec,
            column_specs,
        })
    }

    fn read<R: Read>(reader: &mut R) -> std::io::Result<(Self, u32)> {
        let mut buf_flag = [0u8; 4];
        reader.read_exact(&mut buf_flag)?;
        let flags = i32::from_be_bytes(buf_flag);

        let mut buf_columns_count = [0u8; 4];
        reader.read_exact(&mut buf_columns_count)?;
        let columns_count = i32::from_be_bytes(buf_columns_count);
        let mut bytes_read = 8;

        let global_table_spec: Option<(String, String)>;
        let column_specs: Option<Vec<ColumnSpec>>;

        if flags & RowsMetadaFlagsMask::GlobalTablesSpec as i32
            == RowsMetadaFlagsMask::GlobalTablesSpec as i32
        {
            let (keyspace, read_keyspace) = read_string(reader)?;
            let (table, read_table) = read_string(reader)?;
            bytes_read += read_keyspace + read_table;
            global_table_spec = Some((keyspace, table));
            let mut cols_specs = Vec::new();

            for _ in 0..columns_count {
                let (column_spec, read_column_spec) = ColumnSpec::read(reader)?;
                cols_specs.push(column_spec);
                bytes_read += read_column_spec;
            }
            column_specs = Some(cols_specs);
        } else if flags & RowsMetadaFlagsMask::NoMetadata as i32
            != RowsMetadaFlagsMask::NoMetadata as i32
        {
            return Err(io_error!(
                "Metadata is expected but there is no global table spec"
            ));
        } else {
            global_table_spec = None;
            column_specs = None;
        };

        Ok((
            RowMetadata {
                flags,
                columns_count,
                global_table_spec,
                column_specs,
            },
            bytes_read,
        ))
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<u32> {
        writer.write_all(&self.flags.to_be_bytes())?;
        writer.write_all(&self.columns_count.to_be_bytes())?;
        let mut written = 8;
        if self.flags & RowsMetadaFlagsMask::GlobalTablesSpec as i32
            == RowsMetadaFlagsMask::GlobalTablesSpec as i32
        {
            if let Some((keyspace, table)) = &self.global_table_spec {
                written += write_string(writer, keyspace)?;
                written += write_string(writer, table)?;
            } else {
                return Err(io_error!("Global table spec is None but set to be present"));
            }
            for column in self
                .column_specs
                .as_ref()
                .ok_or(io_error!("No column specs provided but it is mandatory"))?
                .iter()
            {
                written += column.write(writer)?;
            }
        } else if self.flags & RowsMetadaFlagsMask::NoMetadata as i32
            != RowsMetadaFlagsMask::NoMetadata as i32
        {
            return Err(io_error!(
                "Metadata is expected but there is no global table spec"
            ));
        }

        Ok(written)
    }
}

type RowBytes = Vec<Bytes>;
type Row = Vec<String>;

fn strings_to_bytes(input: Vec<String>) -> Vec<Vec<u8>> {
    input
        .into_iter()
        .map(|s| {
            // Intentar convertir a i64 (entero)
            if let Ok(int_val) = s.parse::<i64>() {
                return int_val.to_be_bytes().to_vec();
            }
            // Intentar convertir a f64 (float)
            if let Ok(float_val) = s.parse::<f64>() {
                return float_val.to_be_bytes().to_vec();
            }
            // Convertir a bytes como texto UTF-8
            s.into_bytes()
        })
        .collect()
}

fn vec_row_to_vec_row_bytes(input: Vec<Row>) -> Vec<RowBytes> {
    input
        .into_iter()
        .map(|row| {
            strings_to_bytes(row)
                .into_iter()
                .map(|bytes_data| Bytes { bytes_data })
                .collect()
        })
        .collect()
}

#[derive(Debug)]
pub struct Rows {
    pub metadata: RowMetadata,
    rows_count: i32,
    pub rows_content: Vec<RowBytes>,
}

impl Rows {
    pub fn new(metadata: RowMetadata, rows_count: i32, rows_content: Vec<Row>) -> Self {
        Rows {
            metadata,
            rows_count,
            rows_content: vec_row_to_vec_row_bytes(rows_content),
        }
    }

    pub fn read<R: Read>(reader: &mut R) -> std::io::Result<(Self, u32)> {
        let (metadata, read_metadata) = RowMetadata::read(reader)?;

        let mut buf_rows_count = [0u8; 4];
        reader.read_exact(&mut buf_rows_count)?;
        let rows_count = i32::from_be_bytes(buf_rows_count);
        let mut bytes_read: u32 = read_metadata + 4;

        let mut rows_content: Vec<RowBytes> = Vec::new();

        for _ in 0..rows_count {
            let mut row: RowBytes = Vec::new();
            for _ in 0..metadata.columns_count {
                let (bytes_data, read) = Bytes::read(reader)?;
                row.push(bytes_data);
                bytes_read += read;
            }
            rows_content.push(row);
        }

        Ok((
            Rows {
                metadata,
                rows_count,
                rows_content,
            },
            bytes_read,
        ))
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<u32> {
        let mut written = self.metadata.write(writer)?;
        writer.write_all(&self.rows_count.to_be_bytes())?;
        written += 4;

        for row in self.rows_content.iter() {
            for bytes_type in row.iter() {
                written += bytes_type.write(writer)?;
            }
        }

        Ok(written)
    }
}

#[derive(Debug)]
pub enum ResultOP {
    Void, // Void = 0x0001
    Rows(Rows), // Rows = 0x0002
          // SetKeyspace = 0x0003
          // Prepared = 0x0004
          // SchemaChange = 0x0005
}

impl ResultOP {
    pub fn new(kind: i32, rows: Option<Rows>) -> std::io::Result<Self> {
        match kind {
            0x0001 => Ok(ResultOP::Void),
            0x0002 => {
                if let Some(rows) = rows {
                    Ok(ResultOP::Rows(rows))
                } else {
                    Err(io_error!("Rows is None"))
                }
            }
            _ => Err(io_error!(format!("Invalid result kind: {kind}"))),
        }
    }

    /// Reads the result from the reader. The result can be a void or rows.
    ///
    /// # Arguments
    ///
    /// * `reader` - A reference to the reader.
    /// * `length` - The length of the result.
    ///
    /// # Returns
    ///
    /// * Returns the `ResultOP` representing the result message
    ///
    /// # Errors
    ///
    /// * Returns an error if there is an issue reading the result.
    pub fn read<R: Read>(reader: &mut R, length: u32) -> std::io::Result<Self> {
        let mut buffer = vec![0u8; length as usize];
        reader.read_exact(&mut buffer)?;
        let mut reader = Cursor::new(buffer);
        let mut buf_kind = [0u8; 4];
        reader.read_exact(&mut buf_kind)?;
        let kind = i32::from_be_bytes(buf_kind);
        let mut bytes_read = 4;

        match kind {
            0x0001 => {
                if bytes_read != length {
                    Err(io_error!("Body length is greater than the frame length"))
                } else {
                    Ok(ResultOP::Void)
                }
            }
            0x0002 => {
                let (rows, read_rows) = Rows::read(&mut reader)?;
                bytes_read += read_rows;
                if bytes_read != length {
                    Err(io_error!("Body length is greater than the frame length"))
                } else {
                    Ok(ResultOP::Rows(rows))
                }
            }
            _ => Err(io_error!(format!("Invalid result kind: {kind}"))),
        }
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<u32> {
        let kind: i32 = match self {
            ResultOP::Void => 0x0001,
            ResultOP::Rows(_) => 0x0002,
        };
        writer.write_all(&kind.to_be_bytes())?;
        match self {
            ResultOP::Void => Ok(4),
            ResultOP::Rows(rows) => Ok(rows.write(writer)? + 4),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, vec};

    use super::*;

    #[test]
    fn test_read_column_spec() {
        let mut buffer = Cursor::new(vec![0x00, 0x04, b'n', b'a', b'm', b'e', 0x0, 0x000D]);
        let (colum_spec, read) = ColumnSpec::read(&mut buffer).unwrap();
        assert_eq!(colum_spec.name, "name".to_string());
        assert_eq!(colum_spec.data_type, DataTypeFlags::Varchar);
        assert_eq!(read, 8)
    }

    #[test]
    fn test_write_column_spec() {
        let mut buffer: Vec<u8> = Vec::new();
        let column_spec = ColumnSpec::new("name".to_string(), DataTypeFlags::Varchar);
        assert_eq!(column_spec.write(&mut buffer).unwrap(), 8);
        let expected = vec![0x00, 0x04, b'n', b'a', b'm', b'e', 0x0, 0x000D];
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_read_and_write_column_spec() {
        let col_spec = ColumnSpec::new("name".to_string(), DataTypeFlags::Varchar);

        let mut buffer: Vec<u8> = Vec::new();
        let written = col_spec.write(&mut buffer).unwrap();
        assert_eq!(written, 8);

        let mut buffer = Cursor::new(buffer);
        let (col_spec, read) = ColumnSpec::read(&mut buffer).unwrap();
        assert_eq!(read, 8);

        assert_eq!(col_spec.name, "name".to_string());
        assert_eq!(col_spec.data_type, DataTypeFlags::Varchar);
    }

    #[test]
    fn test_read_row_metadata() {
        let content = vec![
            0x00, 0x00, 0x00, 0x01, // 0x0001 => flags
            0x00, 0x00, 0x00, 0x02, // 0x0002 => columns_count
            0x00, 0x03, b'k', b'e', b'y', // keyspace
            0x00, 0x05, b't', b'a', b'b', b'l', b'e', // table
            0x00, 0x04, b'n', b'a', b'm', b'e', 0x00, 0x0D, // column_spec: name varchar
            0x00, 0x04, b'a', b'l', b'g', b'o', 0x00, 0x0D, // column_spec: algo varchar
        ];

        let mut buffer = Cursor::new(&content);

        let (metadata, read) = RowMetadata::read(&mut buffer).unwrap();
        assert_eq!(read, content.len() as u32);
        assert_eq!(metadata.flags, 1);
        assert_eq!(metadata.columns_count, 2);
        assert_eq!(
            metadata.global_table_spec,
            Some(("key".to_string(), "table".to_string()))
        );
        assert_eq!(
            metadata.column_specs.as_ref().unwrap()[0].name,
            "name".to_string()
        );
        assert_eq!(metadata.column_specs.unwrap()[1].name, "algo".to_string());
    }

    #[test]
    fn test_write_row_metadata() {
        let mut column_specs: Vec<ColumnSpec> = Vec::new();

        let column_spec_1 = ColumnSpec::new("name".to_string(), DataTypeFlags::Varchar);

        let column_spec_2 = ColumnSpec::new("algo".to_string(), DataTypeFlags::Varchar);

        column_specs.push(column_spec_1);
        column_specs.push(column_spec_2);

        let mut buffer: Vec<u8> = Vec::new();
        let row_metadata = RowMetadata::new(
            0x01,
            2,
            Some(("key".to_string(), "table".to_string())),
            Some(column_specs),
        )
        .unwrap();

        row_metadata.write(&mut buffer).unwrap();
        let expected = vec![
            0x00, 0x00, 0x00, 0x01, // 0x0001 => flags
            0x00, 0x00, 0x00, 0x02, // 0x0002 => columns_count
            0x00, 0x03, b'k', b'e', b'y', // keyspace
            0x00, 0x05, b't', b'a', b'b', b'l', b'e', // table
            0x00, 0x04, b'n', b'a', b'm', b'e', 0x00, 0x0D, // column_spec: name varchar
            0x00, 0x04, b'a', b'l', b'g', b'o', 0x00, 0x0D, // column_spec: algo varchar
        ];

        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_read_and_write_row_metadata() {
        let column_spec_1 = ColumnSpec::new("name".to_string(), DataTypeFlags::Varchar);
        let column_spec_2 = ColumnSpec::new("algo".to_string(), DataTypeFlags::Varchar);
        let column_specs: Vec<ColumnSpec> = vec![column_spec_1, column_spec_2];

        let row_metadata = RowMetadata::new(
            0x01,
            2,
            Some(("key".to_string(), "table".to_string())),
            Some(column_specs),
        )
        .unwrap();

        let mut buffer: Vec<u8> = Vec::new();
        let written = row_metadata.write(&mut buffer).unwrap();
        assert_eq!(written, 36);

        let mut buffer = Cursor::new(buffer);
        let (row_metadata, read) = RowMetadata::read(&mut buffer).unwrap();
        assert_eq!(read, 36);

        assert_eq!(row_metadata.flags, 1);
        assert_eq!(row_metadata.columns_count, 2);
        assert_eq!(
            row_metadata.global_table_spec,
            Some(("key".to_string(), "table".to_string()))
        );
        assert_eq!(row_metadata.column_specs.as_ref().unwrap().len() as u32, 2);
        assert_eq!(
            row_metadata.column_specs.as_ref().unwrap()[0].name,
            "name".to_string()
        );
        assert_eq!(
            row_metadata.column_specs.unwrap()[1].name,
            "algo".to_string()
        );
    }

    #[test]
    fn test_read_rows() {
        let content = vec![
            //metadata
            0x00, 0x00, 0x00, 0x01, // 0x0001 => flags
            0x00, 0x00, 0x00, 0x02, // 0x0002 => columns_count
            0x00, 0x03, b'k', b'e', b'y', 0x00, 0x05, b't', b'a', b'b', b'l',
            b'e', // key, table
            0x00, 0x04, b'n', b'a', b'm', b'e', 0x0, 0x0D, 0x00, 0x04, b'a', b'l', b'g', b'o',
            0x00, 0x0D, // column_specs
            //row_count
            0x00, 0x00, 0x00, 0x03, //row_content
            //row1
            0x00, 0x00, 0x00, 0x05, b'h', b'e', b'l', b'l', b'o', // name: hello
            0x00, 0x00, 0x00, 0x05, b'w', b'o', b'r', b'l', b'd', // algo: world
            //row2
            0x00, 0x00, 0x00, 0x03, b'd', b'o', b'g', // name: dog
            0x00, 0x00, 0x00, 0x04, b'g', b'u', b'a', b'u', // algo: guau
            //row3
            0x00, 0x00, 0x00, 0x04, b'g', b'o', b'o', b'd', // name: good
            0x00, 0x00, 0x00, 0x03, b'b', b'y', b'e', // algo: bye
        ];
        let mut buffer = Cursor::new(&content);

        let (rows, read) = Rows::read(&mut buffer).unwrap();
        assert_eq!(read, content.len() as u32);
        assert_eq!(rows.rows_count, 3);
        assert_eq!(rows.rows_content.len(), 3);
        assert_eq!(rows.metadata.flags, 1);
        assert_eq!(rows.metadata.columns_count, 2);
        assert_eq!(
            rows.metadata.global_table_spec,
            Some(("key".to_string(), "table".to_string()))
        );
        assert_eq!(
            rows.rows_content[0][0].bytes_data,
            vec![b'h', b'e', b'l', b'l', b'o']
        );
        assert_eq!(
            rows.rows_content[0][1].bytes_data,
            vec![b'w', b'o', b'r', b'l', b'd']
        );
        assert_eq!(rows.rows_content[1][0].bytes_data, vec![b'd', b'o', b'g']);
        assert_eq!(
            rows.rows_content[1][1].bytes_data,
            vec![b'g', b'u', b'a', b'u']
        );
        assert_eq!(
            rows.rows_content[2][0].bytes_data,
            vec![b'g', b'o', b'o', b'd']
        );
        assert_eq!(rows.rows_content[2][1].bytes_data, vec![b'b', b'y', b'e']);
    }

    #[test]
    fn test_write_rows() {
        let column_spec_1 = ColumnSpec::new("name".to_string(), DataTypeFlags::Varchar);
        let column_spec_2 = ColumnSpec::new("algo".to_string(), DataTypeFlags::Varchar);
        let column_specs: Vec<ColumnSpec> = vec![column_spec_1, column_spec_2];

        let row_metadata = RowMetadata::new(
            0x01,
            2,
            Some(("key".to_string(), "table".to_string())),
            Some(column_specs),
        )
        .unwrap();

        let rows_content = vec![vec!["hello".to_string(), "world".to_string()]];

        let rows = Rows::new(row_metadata, 1, rows_content);
        let mut buffer: Vec<u8> = Vec::new();
        rows.write(&mut buffer).unwrap();

        let expected = vec![
            //metadata
            0x00, 0x00, 0x00, 0x01, // 0x0001 => flags
            0x00, 0x00, 0x00, 0x02, // 0x0002 => columns_count
            0x00, 0x03, b'k', b'e', b'y', 0x00, 0x05, b't', b'a', b'b', b'l',
            b'e', // key, table
            0x00, 0x04, b'n', b'a', b'm', b'e', 0x0, 0x0D, 0x00, 0x04, b'a', b'l', b'g', b'o',
            0x00, 0x0D, // column_specs
            //row_count
            0x00, 0x00, 0x00, 0x01, //row_content
            //row1
            0x00, 0x00, 0x00, 0x05, b'h', b'e', b'l', b'l', b'o', // name: hello
            0x00, 0x00, 0x00, 0x05, b'w', b'o', b'r', b'l', b'd', // algo: world
        ];

        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_read_and_write_rows() {
        let col_spec_1 = ColumnSpec::new("name".to_string(), DataTypeFlags::Varchar);
        let col_spec_2 = ColumnSpec::new("algo".to_owned(), DataTypeFlags::Varchar);
        let col_specs = vec![col_spec_1, col_spec_2];

        let row_metadata = RowMetadata::new(
            0x01,
            2,
            Some(("key".to_string(), "table".to_string())),
            Some(col_specs),
        );

        let rows_content = vec![vec!["hello".to_string(), "world".to_string()]];

        let rows = Rows::new(row_metadata.unwrap(), 1, rows_content);

        let mut buffer: Vec<u8> = Vec::new();
        let written = rows.write(&mut buffer).unwrap();
        assert_eq!(written, 58);

        let mut buffer = Cursor::new(buffer);
        let (rows, read) = Rows::read(&mut buffer).unwrap();
        assert_eq!(read, 58);

        assert_eq!(rows.rows_count, 1);
        assert_eq!(rows.metadata.flags, 1);
        assert_eq!(rows.metadata.columns_count, 2);
        assert_eq!(
            rows.metadata.global_table_spec,
            Some(("key".to_string(), "table".to_string()))
        );
        assert_eq!(rows.rows_content.len(), 1);
        assert_eq!(
            rows.rows_content[0][0].bytes_data,
            vec![b'h', b'e', b'l', b'l', b'o']
        );
        assert_eq!(
            rows.rows_content[0][1].bytes_data,
            vec![b'w', b'o', b'r', b'l', b'd']
        );
    }

    #[test]
    fn test_read_and_write_result() {
        let col_spec_1 = ColumnSpec::new("name".to_string(), DataTypeFlags::Varchar);
        let col_spec_2 = ColumnSpec::new("age".to_owned(), DataTypeFlags::Int);
        let col_spec_3 = ColumnSpec::new("email".to_owned(), DataTypeFlags::Varchar);
        let col_specs = vec![col_spec_1, col_spec_2, col_spec_3];

        let row_metadata = RowMetadata::new(
            0x01,
            3,
            Some(("ks_test".to_string(), "table_test".to_string())),
            Some(col_specs),
        );

        let rows_content = vec![
            vec!["hello".to_string(), "25".to_string(), "email".to_string()],
            vec!["world".to_string(), "30".to_string(), "email".to_string()],
        ];

        let rows = Rows::new(row_metadata.unwrap(), 2, rows_content);
        let result_op = ResultOP::new(0x0002, Some(rows)).unwrap();

        let mut buffer: Vec<u8> = Vec::new();
        let written = result_op.write(&mut buffer).unwrap();
        assert_eq!(written, 121);

        let mut buffer = Cursor::new(buffer);
        let result_op = ResultOP::read(&mut buffer, 121).unwrap();

        match result_op {
            ResultOP::Rows(rows) => {
                assert_eq!(rows.rows_count, 2);
                assert_eq!(rows.metadata.flags, 1);
                assert_eq!(rows.metadata.columns_count, 3);
                assert_eq!(
                    rows.metadata.global_table_spec,
                    Some(("ks_test".to_string(), "table_test".to_string()))
                );
                assert_eq!(rows.rows_content.len(), 2);
                assert_eq!(
                    rows.rows_content[0][0].bytes_data,
                    vec![b'h', b'e', b'l', b'l', b'o']
                );
                assert_eq!(
                    rows.rows_content[0][1].bytes_data,
                    vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x19]
                );
                assert_eq!(
                    rows.rows_content[0][2].bytes_data,
                    vec![b'e', b'm', b'a', b'i', b'l']
                );
                assert_eq!(
                    rows.rows_content[1][0].bytes_data,
                    vec![b'w', b'o', b'r', b'l', b'd']
                );
                assert_eq!(
                    rows.rows_content[1][1].bytes_data,
                    vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1E]
                );
                assert_eq!(
                    rows.rows_content[1][2].bytes_data,
                    vec![b'e', b'm', b'a', b'i', b'l']
                );
            }
            _ => panic!("Should be a Rows result"),
        }
    }

    #[test]
    fn test_read_result_with_void() {
        let mut buffer = Cursor::new(vec![0x00, 0x00, 0x00, 0x01]);
        let result_op = ResultOP::read(&mut buffer, 4).unwrap();
        match result_op {
            ResultOP::Rows(_) => panic!("Should be a Void result"),
            ResultOP::Void => {}
        }
    }

    #[test]
    fn test_write_result_with_void() {
        let result_op = ResultOP::new(0x01, None).unwrap();
        let mut buffer: Vec<u8> = Vec::new();
        let written = result_op.write(&mut buffer).unwrap();
        assert_eq!(written, 4);
        let expected = vec![0x0, 0x0, 0x0, 0x1];
        assert_eq!(expected, buffer);
    }
}
