use crate::native_protocol::parsers::{
    bytes_type::{read_bytes, write_bytes, BytesType},
    string::{read_string, write_string},
};

use std::io::{Cursor, Read, Write};
#[derive(Debug,PartialEq)]
pub enum ResultKindFlags {
    Void = 0x0001,
    Rows = 0x0002,
    // SetKeyspace = 0x0003,
    // Prepared = 0x0004 ,
    // SchemaChange = 0x0005,
}

impl ResultKindFlags {
    pub fn new(result_kind: i32) -> std::io::Result<ResultKindFlags> {
        match result_kind {
            0x0001 => Ok(ResultKindFlags::Void),
            0x0002 => Ok(ResultKindFlags::Rows),
            _ => std::io::Result::Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid kind: {result_kind}"),
            )),
        }
    }
    pub fn to_be_bytes(&self) -> [u8; 4] {
        match self {
            ResultKindFlags::Void => [0x0, 0x0, 0x0, 0x01],
            ResultKindFlags::Rows => [0x0, 0x0, 0x0, 0x02],
        }
    }
}

#[derive(Debug)]
pub struct ResultOP {
    pub kind: ResultKindFlags,
    pub rows: Option<Rows>,
}

impl ResultOP {
    pub fn new(kind: ResultKindFlags, rows: Option<Rows>) -> Result<Self, String> {
        match kind {
            ResultKindFlags::Void => {
                if let Some(_) = rows {
                    return Err("rows not empty".to_string());
                }
                return Ok(ResultOP { kind, rows: None });
            }
            ResultKindFlags::Rows => {
                if let Some(_) = rows {
                    return Ok(ResultOP { kind, rows });
                }
                return Err("rows is empty".to_string());
            }
        }
    }

    pub fn read_result<R: Read>(reader: &mut R, length: u32) -> std::io::Result<(ResultOP,u32)> {
        let mut bytes_read:u32 = 0;
        
        let mut buf_kind = [0u8; 4];
        reader.read_exact(&mut buf_kind)?;
        let kind_num = i32::from_be_bytes(buf_kind);
        bytes_read += 4;

        let kind = ResultKindFlags::new(kind_num)?;
        match kind {
            ResultKindFlags::Void => Ok((ResultOP { kind, rows: None },bytes_read)),
            ResultKindFlags::Rows => {
                //length - 4 is length rows
                let (rows,read_rows) = Rows::read_rows(reader, length - 4)?;
                bytes_read += read_rows;
                Ok((
                    ResultOP {
                    kind,
                    rows: Some(rows),
                    },
                    bytes_read
                ))
            }
        }
    }

    pub fn write_result<W: Write>(&self, writer: &mut W) -> std::io::Result<u32>{
        let mut bytes_result:Vec<u8> = Vec::new();
        bytes_result.write_all(&self.kind.to_be_bytes()) ?;
        match self.kind {
            ResultKindFlags::Void => {
                let length = 4 as u32;
                writer.write_all(&length.to_be_bytes())?;
                writer.write_all(&bytes_result)?;
                Ok(4)
            }
            ResultKindFlags::Rows => {
                if let Some(rows) = &self.rows{
                    rows.write_rows(&mut bytes_result)?;
                    let length = bytes_result.len() as u32;
                    writer.write_all(&length.to_be_bytes() )?;
                    writer.write_all(&bytes_result)?;
                    return Ok(length);
                }
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Empty row",
                ))
            }
        }
    }
}
#[derive(Debug)]
pub struct Rows {
    pub metadata: RowMetadata,
    rows_count: i32,
    pub rows_content: Vec<Row>,
}
impl Rows {
    pub fn new(metadata: RowMetadata, rows_count: i32, rows_content: Vec<Row>) -> Self {
        Rows {
            metadata,
            rows_count,
            rows_content,
        }
    }
    pub fn read_rows<R: Read>(reader: &mut R, length: u32) -> std::io::Result<(Rows,u32)> {
        let mut bytes_read:u32 = 0; 

        let mut buffer = vec![0; length as usize];
        reader.read_exact(&mut buffer)?;

        let mut cursor = Cursor::new(buffer);

        let (metadata,read_metadata) = RowMetadata::read_metadata_row(&mut cursor)?;
        bytes_read += read_metadata;

        let mut buf_rows_count = [0u8; 4];
        cursor.read_exact(&mut buf_rows_count)?;
        let rows_count = i32::from_be_bytes(buf_rows_count);
        bytes_read += 4;

        let mut rows_content: Vec<Row> = Vec::new();

        for _ in 0..rows_count {
            let row: Row;
            let mut row_actual: Vec<BytesType> = Vec::new();
            for _ in 0..metadata.columns_count {
                let (bytes_data, read) = read_bytes(&mut cursor)?;
                row_actual.push(bytes_data);
                bytes_read += read;
            }
            row = Row::new(row_actual);
            rows_content.push(row);
        }

        Ok((
            Rows {
            metadata,
            rows_count,
            rows_content,
            },
            bytes_read
        ))
    }

    pub fn write_rows<W: Write>(&self, writer: &mut W) -> std::io::Result<u32>{
        let mut bytes_rows:Vec<u8> = Vec::new();
        self.metadata.write_row_metadata(&mut bytes_rows)?;
        bytes_rows.write_all(&self.rows_count.to_be_bytes())?;
        
        for row in self.rows_content.iter(){
            for bytes_type in row.values.iter(){
                write_bytes(&mut bytes_rows, bytes_type.length, bytes_type.bytes_data.to_owned())?;
            }
        }
        let length = bytes_rows.len() as u32;

        writer.write_all(&bytes_rows)?;

        Ok(length)
    }
}

#[derive(Debug)]
pub struct RowMetadata {
    pub flags: i32,
    pub columns_count: i32,
    pub paging_state: Option<Vec<u8>>,
    pub global_table_spec: Option<(String, String)>, // (keyspace, table)
    pub column_specs: Vec<ColumnSpec>,
}

impl RowMetadata {
    pub fn new(
        flags: i32,
        columns_count: i32,
        paging_state: Option<Vec<u8>>,
        global_table_spec: Option<(String, String)>,
        column_specs: Vec<ColumnSpec>,
    ) -> Result<Self, String> {
        match flags {
            0x0001 => {
                if let None = global_table_spec {
                    return Err("Global table spec is None".to_string());
                }
            }
            0x0002 => {
                if let None = paging_state {
                    return Err("Paging state spec is None".to_string());
                }
            }
            // 0x0004 =>{
            //     if let None = global_table_spec{
            //         return Err("Global table spec is None".to_string());
            //     }
            // }
            _ => {
                return Err("row metadata flag invalid.".to_string());
            }
        }
        Ok(RowMetadata {
            flags,
            columns_count,
            paging_state,
            global_table_spec,
            column_specs,
        })
    }

    pub fn read_metadata_row<R: Read>(reader: &mut R) -> std::io::Result<(RowMetadata,u32)> {
        let mut bytes_read:u32 = 0;
        
        let mut buf_flag = [0u8; 4];
        reader.read_exact(&mut buf_flag)?;
        let flags = i32::from_be_bytes(buf_flag);
        bytes_read += 4;

        let mut buf_columns_count = [0u8; 4];
        reader.read_exact(&mut buf_columns_count)?;
        let columns_count = i32::from_be_bytes(buf_columns_count);
        bytes_read +=4;

        let global_table_spec: Option<(String, String)>;
        let paging_state: Option<Vec<u8>> = None;

        if flags == 0x0001 {
            let (keyspace, read_keyspace) = read_string(reader)?;
            let (table, read_table) = read_string(reader)?;
            global_table_spec = Some((keyspace, table));
            bytes_read = bytes_read + read_keyspace +read_table;
        } else {
            global_table_spec = None;
        }
        let mut column_specs: Vec<ColumnSpec> = Vec::new();
        for _ in 0..columns_count {
            let (column_spec,read_column_spec) = ColumnSpec::read_column_spec(reader, flags)?;
            column_specs.push(column_spec);
            bytes_read += read_column_spec;
        }

        Ok((
            RowMetadata {
                flags,
                columns_count,
                paging_state,
                global_table_spec,
                column_specs,
            },
            bytes_read
        ))

    }

    pub fn write_row_metadata<W: Write>(&self, writer: &mut W) -> std::io::Result<u32>{
        let mut bytes_row_metadata:Vec<u8> = Vec::new();
        bytes_row_metadata.write_all(&self.flags.to_be_bytes())?;
        bytes_row_metadata.write_all(&self.columns_count.to_be_bytes())?;
        if self.flags == 0x01{
            if let Some((keyspace,table)) = &self.global_table_spec{
                write_string(&mut bytes_row_metadata, &keyspace)?;
                write_string(&mut bytes_row_metadata, &table)?;
            }
        }
        for column in self.column_specs.iter(){
            column.write_column_spec(&mut bytes_row_metadata, self.flags)?;
        }
        let length = bytes_row_metadata.len() as u32;
        writer.write_all(&bytes_row_metadata)?;

        Ok(length)
    }
}

#[derive(Debug)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: DataTypeFlags,
    pub ksname: Option<String>, // Optional keyspace name if not using global_table_spec
    pub tablename: Option<String>, // Optional table name if not using global_table_spec
}

impl ColumnSpec {
    pub fn new(
        name: String,
        data_type: DataTypeFlags,
        ksname: Option<String>,
        tablename: Option<String>,
    ) -> Result<Self, String> {
        Ok(ColumnSpec {
            name,
            data_type,
            ksname,
            tablename,
        })
    }

    pub fn read_column_spec<R: Read>(reader: &mut R, flag: i32) -> std::io::Result<(ColumnSpec,u32)> {
        let ksname: Option<String>;
        let tablename: Option<String>;

        let mut bytes_read:u32 = 0; 
        if flag == 0x0001 {
            ksname = None;
            tablename = None;
        } else {
            let (ks, read_ks) = read_string(reader)?;
            let (tb, read_tb) = read_string(reader)?;
            ksname = Some(ks);
            tablename = Some(tb);
            bytes_read = read_ks + read_tb;
        }
        let (name, read_name) = read_string(reader)?;
        bytes_read += read_name;
        
        let mut buf_data_type = [0u8; 2];
        reader.read_exact(&mut buf_data_type)?;
        let data_type_u16 = u16::from_be_bytes(buf_data_type);
        let data_type = DataTypeFlags::new(data_type_u16)?;
        bytes_read += 2;

        Ok((
            ColumnSpec {
            name,
            data_type,
            ksname,
            tablename,
            },
            bytes_read
        ))
    }

    pub fn write_column_spec<W: Write>(&self,writer:&mut W,flag: i32) -> std::io::Result<u32>{
        let mut bytes_column_spec: Vec<u8> = Vec::new();

        if flag != 0x01{

            if let None = &self.ksname{
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("ksname empty"),
                ))
            }
            if let None = &self.tablename{
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("tablename empty"),
                ))
            }

            if let Some(ks) = &self.ksname{
                write_string(&mut bytes_column_spec, ks)?;
            }
            if let Some(tb) = &self.tablename{
                write_string(&mut bytes_column_spec, tb)?;
            }
            
        }
        write_string(&mut bytes_column_spec, &self.name)?;

        let buf_data_type = self.data_type.to_be_bytes();
        bytes_column_spec.write_all(&buf_data_type)?;

        let length = bytes_column_spec.len() as u32;
        writer.write_all(&mut bytes_column_spec)?;
        Ok(length)
    }
}

#[derive(Debug)]
pub struct Row {
    pub values: Vec<BytesType>, // Each row contains values for each column
}

impl Row {
    pub fn new(values: Vec<BytesType>) -> Self {
        Row { values }
    }
}

// pub struct PreparedMetadata {
//     pub flags: i32,
//     pub columns_count: i32,
//     pub pk_count: i32,
//     pub pk_indices: Vec<u16>,
//     pub global_table_spec: Option<(String, String)>, // (keyspace, table)
//     pub column_specs: Vec<ColumnSpec>,
// }

#[derive(Debug, PartialEq)]
pub enum DataTypeFlags {
    // Bigint = 0x0002,
    // Blob = 0x0003,
    Boolean = 0x0004,
    Counter = 0x0005,
    // Decimal = 0x0006,
    Double = 0x0007,
    Float = 0x0008,
    Int = 0x0009,
    Timestamp = 0x000B,
    Uuid = 0x000C,
    Varchar = 0x000D,
    Varint = 0x000E,
    Timeuuid = 0x000F,
    Inet = 0x0010,
    Date = 0x0011,
    Time = 0x0012,
    // Smallint = 0x0013,
    // Tinyint = 0x0014,
}

impl DataTypeFlags {
    pub fn new(flag: u16) -> std::io::Result<DataTypeFlags> {
        match flag {
            0x0004 => Ok(DataTypeFlags::Boolean),
            0x0005 => Ok(DataTypeFlags::Counter),

            0x0007 => Ok(DataTypeFlags::Double),
            0x0008 => Ok(DataTypeFlags::Float),
            0x0009 => Ok(DataTypeFlags::Int),
            0x000B => Ok(DataTypeFlags::Timestamp),
            0x000C => Ok(DataTypeFlags::Uuid),
            0x000D => Ok(DataTypeFlags::Varchar),
            0x000E => Ok(DataTypeFlags::Varint),
            0x000F => Ok(DataTypeFlags::Timeuuid),
            0x0010 => Ok(DataTypeFlags::Inet),
            0x0011 => Ok(DataTypeFlags::Date),
            0x0012 => Ok(DataTypeFlags::Time),
            _ => std::io::Result::Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid data flag: {flag}"),
            )),
        }
    }
    pub fn to_be_bytes(&self) -> [u8; 2] {
        match self {
            DataTypeFlags::Boolean => [0x0, 0x0004],
            DataTypeFlags::Counter => [0x0, 0x0005],
            DataTypeFlags::Double => [0x0, 0x0007],
            DataTypeFlags::Float => [0x0, 0x0008],
            DataTypeFlags::Int => [0x0, 0x0009],
            DataTypeFlags::Timestamp => [0x0, 0x000B],
            DataTypeFlags::Uuid => [0x0, 0x000C],
            DataTypeFlags::Varchar => [0x0, 0x000D],
            DataTypeFlags::Varint => [0x0, 0x000E],
            DataTypeFlags::Timeuuid => [0x0, 0x00F],
            DataTypeFlags::Inet => [0x0, 0x0010],
            DataTypeFlags::Date => [0x0, 0x0011],
            DataTypeFlags::Time => [0x0, 0x0012],
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_read_column_spec_with_flag_global() {
        let mut buffer = Cursor::new(vec![0x00, 0x04, b'n', b'a', b'm', b'e', 0x0, 0x000D]);
        let flag = 0x0001;
        let (colum_spec,read) = ColumnSpec::read_column_spec(&mut buffer, flag).unwrap();
        assert_eq!(colum_spec.name, "name".to_string());
        assert_eq!(colum_spec.data_type, DataTypeFlags::Varchar);
        assert_eq!(colum_spec.ksname, None);
        assert_eq!(colum_spec.tablename, None);
        assert_eq!(read,8)
    }

    #[test]
    fn test_read_row_metadata_with_global() {
        let content = vec![
            0x0, 0x0, 0x0, 0x0001, 0x0, 0x0, 0x0, 0x02, 0x00, 0x03, b'k', b'e', b'y', 0x00, 0x05,
            b't', b'a', b'b', b'l', b'e', 0x00, 0x04, b'n', b'a', b'm', b'e', 0x0, 0x000D, 0x00,
            0x04, b'a', b'l', b'g', b'o', 0x0, 0x000D,
        ];

        let expected_read = content.len() as u32;

        let mut buffer = Cursor::new(content);

        let (metadata,read) = RowMetadata::read_metadata_row(&mut buffer).unwrap();
        assert_eq!(metadata.flags, 1);
        //assert_eq!(colum_spec.data_type,DataTypeFlags::Varchar);
        assert_eq!(metadata.columns_count, 2);
        assert_eq!(
            metadata.global_table_spec,
            Some(("key".to_string(), "table".to_string()))
        );
        assert_eq!(metadata.column_specs[0].name, "name".to_string());
        assert_eq!(metadata.column_specs[1].name, "algo".to_string());
        assert_eq!(metadata.paging_state, None);
        assert_eq!(read,expected_read);
    }

    #[test]
    fn test_read_rows() {
        let content = vec![
            //metadata
            0x0, 0x0, 0x0, 0x0001, //column_count
            0x0, 0x0, 0x0, 0x02, //
            0x00, 0x03, b'k', b'e', b'y', 0x00, 0x05, b't', b'a', b'b', b'l', b'e', 0x00, 0x04,
            b'n', b'a', b'm', b'e', 0x0, 0x000D, 0x00, 0x04, b'a', b'l', b'g', b'o', 0x0, 0x000D,
            //row_count
            0x0, 0x0, 0x0, 0x03, //row_content
            //row1
            0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03, 0x04, 0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03,
            0x04, //row2
            0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03, 0x04, 0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03,
            0x04, //row3
            0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03, 0x04, 0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03,
            0x04,
        ];
        let length = content.len() as u32;
        let mut buffer = Cursor::new(content);

        let (rows,read) = Rows::read_rows(&mut buffer, length).unwrap();
        assert_eq!(rows.rows_count, 3);
        assert_eq!(rows.rows_content.len(), 3);
        assert_eq!(read,length)
    }

    #[test]
    fn test_read_result_with_rows() {

        let contenido = vec![
            //kind
            0x0, 0x0, 0x0, 0x02,

            //metadata
            0x0, 0x0, 0x0, 0x0001, //column_count
            0x0, 0x0, 0x0, 0x02, //
            0x00, 0x03, b'k', b'e', b'y', 0x00, 0x05, b't', b'a', b'b', b'l', b'e', 0x00, 0x04,
            b'n', b'a', b'm', b'e', 0x0, 0x000D, 0x00, 0x04, b'a', b'l', b'g', b'o', 0x0, 0x000D,
            //row_count
            0x0, 0x0, 0x0, 0x03, //row_content
            //row1
            0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03, 0x04, 0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03,
            0x04, //row2
            0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03, 0x04, 0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03,
            0x04, //row3
            0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03, 0x04, 0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03,
            0x04,
        ];
        let length = contenido.len() as u32;
        let mut buffer = Cursor::new(contenido);
        let (result_op,read) = ResultOP::read_result(&mut buffer, length).unwrap();
        assert_eq!(result_op.kind,ResultKindFlags::Rows);
        assert_eq!(result_op.rows.is_none(),false);
        assert_eq!(read,length);
    }

    #[test]
    fn test_read_result_with_void() {
        let mut buffer = Cursor::new(vec![0x00,0x00,0x00,0x01]);
        let length = 4;
        let (result_op,read) = ResultOP::read_result(&mut buffer, length).unwrap();
        assert_eq!(result_op.kind,ResultKindFlags::Void);
        assert!(result_op.rows.is_none());
        assert_eq!(read,length)
    }

    #[test]
    fn test_write_column_spec_with_flag_global() {
        let mut buffer: Vec<u8> = Vec::new();
        let column_spec = ColumnSpec::new(
            "name".to_string(),
            DataTypeFlags::Varchar, 
            None, None).unwrap();
        column_spec.write_column_spec(&mut buffer, 0x01).unwrap();
        let expected = vec![0x00, 0x04, b'n', b'a', b'm', b'e', 0x0, 0x000D];
        assert_eq!(buffer,expected);
    }
    
    #[test]
    fn test_write_row_metadata_with_global() {
        
        let mut column_specs:Vec<ColumnSpec> = Vec::new();

        let column_spec_1 = ColumnSpec::new(
            "name".to_string(),
            DataTypeFlags::Varchar, 
            None, None).unwrap();

        let column_spec_2 = ColumnSpec::new(
            "algo".to_string(),
            DataTypeFlags::Varchar, 
            None, None).unwrap();
        
        column_specs.push(column_spec_1);
        column_specs.push(column_spec_2);

        let mut buffer: Vec<u8> = Vec::new();
        let row_metadata = RowMetadata::new(
            0x01, 
            2, 
            None, 
            Some(("key".to_string(), "table".to_string())),
            column_specs).unwrap();

        row_metadata.write_row_metadata(&mut buffer).unwrap();
        let expected = vec![
            0x0, 0x0, 0x0, 0x0001, 0x0, 0x0, 0x0, 0x02, 0x00, 0x03, b'k', b'e', b'y', 0x00, 0x05,
            b't', b'a', b'b', b'l', b'e', 0x00, 0x04, b'n', b'a', b'm', b'e', 0x0, 0x000D, 0x00,
            0x04, b'a', b'l', b'g', b'o', 0x0, 0x000D,
        ];

        assert_eq!(buffer,expected);



    }

    #[test]
    fn test_write_rows() {
        
        let mut column_specs:Vec<ColumnSpec> = Vec::new();

        let column_spec_1 = ColumnSpec::new(
            "name".to_string(),
            DataTypeFlags::Varchar, 
            None, None).unwrap();

        let column_spec_2 = ColumnSpec::new(
            "algo".to_string(),
            DataTypeFlags::Varchar, 
            None, None).unwrap();
        
        column_specs.push(column_spec_1);
        column_specs.push(column_spec_2);

        let row_metadata = RowMetadata::new(
            0x01, 
            2, 
            None, 
            Some(("key".to_string(), "table".to_string())),
            column_specs).unwrap();

        let rows_count = 3;

        let mut rows_content: Vec<Row>= Vec::new();

        for _ in 0..rows_count{
            let row: Row;
            let mut row_actual:Vec<BytesType> = Vec::new();
            for _ in 0..2{
                row_actual.push(BytesType { length: 4, bytes_data: vec![1,2,3,4] });
            }
            row = Row::new(row_actual);
            rows_content.push(row);
            
        }

        let rows = Rows::new(row_metadata, rows_count, rows_content);

        let mut buffer:Vec<u8> = Vec::new();

        rows.write_rows(&mut buffer).unwrap();

        let expected = vec![
            //metadata
            0x0, 0x0, 0x0, 0x0001, //column_count
            0x0, 0x0, 0x0, 0x02, //
            0x00, 0x03, b'k', b'e', b'y', 0x00, 0x05, b't', b'a', b'b', b'l', b'e', 0x00, 0x04,
            b'n', b'a', b'm', b'e', 0x0, 0x000D, 0x00, 0x04, b'a', b'l', b'g', b'o', 0x0, 0x000D,
            //row_count
            0x0, 0x0, 0x0, 0x03, //row_content
            //row1
            0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03, 0x04, 0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03,
            0x04, //row2
            0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03, 0x04, 0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03,
            0x04, //row3
            0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03, 0x04, 0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03,
            0x04,
        ];

        assert_eq!(buffer,expected);
        
    }

    #[test]
    fn test_write_result_with_rows() {

        let mut column_specs:Vec<ColumnSpec> = Vec::new();

        let column_spec_1 = ColumnSpec::new(
            "name".to_string(),
            DataTypeFlags::Varchar, 
            None, None).unwrap();

        let column_spec_2 = ColumnSpec::new(
            "algo".to_string(),
            DataTypeFlags::Varchar, 
            None, None).unwrap();
        
        column_specs.push(column_spec_1);
        column_specs.push(column_spec_2);

        
        let row_metadata = RowMetadata::new(
            0x01, 
            2, 
            None, 
            Some(("key".to_string(), "table".to_string())),
            column_specs).unwrap();

        let rows_count = 3;

        let mut rows_content: Vec<Row>= Vec::new();

        for _ in 0..rows_count{
            let row: Row;
            let mut row_actual:Vec<BytesType> = Vec::new();
            for _ in 0..2{
                row_actual.push(BytesType { length: 4, bytes_data: vec![1,2,3,4] });
            }
            row = Row::new(row_actual);
            rows_content.push(row);
            
        }

        let rows = Rows::new(row_metadata, rows_count, rows_content);

        let result_op = ResultOP::new(
            ResultKindFlags::Rows, 
            Some(rows)).unwrap();

        let data_result = vec![
            //kind
            0x0, 0x0, 0x0, 0x02,

            //metadata
            0x0, 0x0, 0x0, 0x0001, //column_count
            0x0, 0x0, 0x0, 0x02, //
            0x00, 0x03, b'k', b'e', b'y', 0x00, 0x05, b't', b'a', b'b', b'l', b'e', 0x00, 0x04,
            b'n', b'a', b'm', b'e', 0x0, 0x000D, 0x00, 0x04, b'a', b'l', b'g', b'o', 0x0, 0x000D,
            //row_count
            0x0, 0x0, 0x0, 0x03, //row_content
            //row1
            0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03, 0x04, 0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03,
            0x04, //row2
            0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03, 0x04, 0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03,
            0x04, //row3
            0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03, 0x04, 0x0, 0x0, 0x0, 0x04, 0x01, 0x02, 0x03,
            0x04,
        ];
        let length = data_result.len() as u32;

        let mut expected:Vec<u8> = Vec::new();
        expected.write_all(&length.to_be_bytes()).unwrap();
        expected.write_all(&data_result).unwrap();
        
        let mut buffer :Vec<u8> = Vec::new();

        result_op.write_result(&mut buffer).unwrap();
        assert_eq!(buffer,expected);
    }

    #[test]
    fn test_write_result_with_void() {
        let expected = vec![0x0,0x0,0x0,0x4,0x0,0x0,0x0,0x1];
        let result_op = ResultOP::new(ResultKindFlags::Void, None).unwrap();
        let mut buffer:Vec<u8> = Vec::new();
        result_op.write_result(&mut buffer).unwrap();
        assert_eq!(expected,buffer);
    }    
    
}
