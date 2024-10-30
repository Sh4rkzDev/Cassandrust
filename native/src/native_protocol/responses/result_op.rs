use std::io::{ Read, Write};
use crate::native_protocol::models::result_op::*;


pub fn read_result<R: Read>(
    reader: &mut R,
    
) -> std::io::Result<(ResultOP,u32)> {
    let mut length_buf = [0u8;4];
    reader.read_exact(&mut length_buf)?;
    let length = u32::from_be_bytes(length_buf);

    let (result_op,read) = ResultOP::read_result(reader, length)?;
    Ok((result_op,read))
}


pub fn write_result<W: Write>(
    writer: &mut W,
    result_op:ResultOP
) -> std::io::Result<u32> {
    let length = result_op.write_result(writer)?;
    Ok(length)

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_protocol::parsers::bytes_type::BytesType;
    use std::io::Cursor;
    #[test]
    fn test_read_result_with_rows() {

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

        let mut buffer = Cursor::new(&expected);
        let (result_op,read) = read_result(&mut buffer).unwrap();
        assert_eq!(result_op.kind,ResultKindFlags::Rows);
        assert_eq!(result_op.rows.is_none(),false);
        assert_eq!(read,length)
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

        write_result(&mut buffer,result_op).unwrap();
        assert_eq!(buffer,expected);
    }
}
