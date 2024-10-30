use std::{fmt::Debug, io::Error};

use super::result_op::{DataTypeFlags, ResultKindFlags, ResultOP};
enum ResponseStatus{
    Ok,
    Error,
}

impl Debug for ResponseStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponseStatus::Ok => write!(f, "Ok"),
            ResponseStatus::Error => write!(f, "Error"),
        }
    }
}

#[derive(Debug)]
pub struct Data{
    pub rows: Vec<Vec<String>>,
    pub columns_names: Vec<String>,
}
#[derive(Debug)]
pub struct Response{
    status: ResponseStatus,
    pub data: Option<Data>,
}    
pub struct ResponseParser{}

impl ResponseParser{
    pub fn parse(&self, response: Result<ResultOP, Error>) -> Response {
        let result_op = if let Ok(result_op) = response{
            result_op
        }else{
            return Response{
                status: ResponseStatus::Error,
                data: None,
            }
        };
        match result_op.kind{
            ResultKindFlags::Void => Response{
                status: ResponseStatus::Ok,
                data: None,
            },
            ResultKindFlags::Rows => {
                let rows = result_op.rows.unwrap();
                let column_names: Vec<String> = rows
                    .metadata
                    .column_specs
                    .iter()
                    .map(|spec| spec.name.clone())
                    .collect();

                let response_rows: Vec<Vec<String>> = rows
                    .rows_content
                    .iter()
                    .map(|row| {
                        row.values
                            .iter()
                            .enumerate()
                            .map(|(index, value)| {

                                let data_type = &rows.metadata.column_specs[index].data_type;

                                match data_type {
                                    DataTypeFlags::Int => {
                                        let int_value :i32 = i32::from_be_bytes(value.bytes_data.clone().try_into().unwrap());
                                        u32::from_str_radix(&int_value.to_string(), 16).unwrap().to_string()
                                    }
                                    DataTypeFlags::Varchar => {
                                        String::from_utf8_lossy(&value.bytes_data).to_string()
                                    }
                                    DataTypeFlags::Boolean => {
                                        let bool_value = value.bytes_data[0] != 0;
                                        bool_value.to_string()
                                    }
                                    DataTypeFlags::Float => {
                                        let float_value = f32::from_bits(u32::from_be_bytes(value.bytes_data.clone().try_into().unwrap()));
                                        float_value.to_string()
                                    }
                                    DataTypeFlags::Double => {
                                        let double_value = f64::from_bits(u64::from_be_bytes(value.bytes_data.clone().try_into().unwrap()));
                                        double_value.to_string()
                                    }
                                    _ => String::from_utf8_lossy(&value.bytes_data).to_string(),
                                }
                            })
                            .collect()
                    })
                    .collect();

                let data = Data{
                    rows: response_rows,
                    columns_names: column_names,
                };
                Response {
                    status: ResponseStatus::Ok,
                    data: Some(data),
                }
            }
        }
    }
} 


#[cfg(test)]
mod tests {
    use crate::native_protocol::{models::result_op::{ColumnSpec, Row, RowMetadata, Rows}, parsers::bytes_type::BytesType};

    use super::*;

    #[test]
    fn test_parse() {
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
                row_actual.push(BytesType { length: 4, bytes_data: vec![b'A',b'C',b'!'] });
            }
            row = Row::new(row_actual);
            rows_content.push(row);
            
        }

        let rows = Rows::new(row_metadata, rows_count, rows_content);

        let result_op = ResultOP::new(
            ResultKindFlags::Rows, 
            Some(rows)).unwrap();

        let parser = ResponseParser{};
        let parsed_response = parser.parse(Ok(result_op));
        println!("{:?}", parsed_response);
        assert!(true)
    }

    #[test]
    fn test_parse_int() {
        let mut column_specs:Vec<ColumnSpec> = Vec::new();

        let column_spec_1 = ColumnSpec::new(
            "name".to_string(),
            DataTypeFlags::Int, 
            None, None).unwrap();

        let column_spec_2 = ColumnSpec::new(
            "algo".to_string(),
            DataTypeFlags::Int, 
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
                row_actual.push(BytesType { length: 4, bytes_data: vec![0,0,0,19] });
            }
            row = Row::new(row_actual);
            rows_content.push(row);
            
        }

        let rows = Rows::new(row_metadata, rows_count, rows_content);

        let result_op = ResultOP::new(
            ResultKindFlags::Rows, 
            Some(rows)).unwrap();

        let parser = ResponseParser{};
        let parsed_response = parser.parse(Ok(result_op));
        println!("{:?}", parsed_response);
        assert!(true)
    }
}