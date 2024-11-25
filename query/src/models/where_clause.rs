use std::collections::HashMap;

use db::Schema;
use serde::{Deserialize, Serialize};
use shared::io_error;

use crate::utils::tokens::tokenize;

/// Represents logical operators that can be used in a `WHERE` clause.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Operator {
    And,
    Or,
    Open,
    Not,
}

/// Indicates whether the comparison should be negated.
type Not = bool;

/// Represents different types of comparison operations in a `WHERE` clause.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum Comparator {
    Equal(String, String, Not),
    GreaterThan(String, String, Not),
    GreaterThanOrEqual(String, String, Not),
    LessThan(String, String, Not),
    LessThanOrEqual(String, String, Not),
}

/// Represents a `WHERE` clause that can be either a comparison or a combination
/// of multiple comparisons using logical operators.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum WhereClause {
    Comp(Comparator),
    Tree(Box<WhereClause>, Operator, Box<WhereClause>),
}

impl WhereClause {
    /// Parses a vector of string parts into a `WhereClause` structure.
    ///
    /// # Parameters
    ///
    /// - `parts`: A slice of strings that represent the query from the `WHERE` clause.
    ///
    /// # Returns
    ///
    /// * Returns a tuple containing the parsed `WhereClause` and an optional keyword index
    ///
    /// # Errors
    ///
    /// * Returns an `Error` if the function encounters an invalid syntax.
    pub(crate) fn new(parts: &[String]) -> std::io::Result<(Self, Option<usize>)> {
        let (tokens, keyword) = tokenize(parts, |s| s != "AND" && s != "OR" && s != "NOT")?;
        let mut out = Vec::new();
        let mut ops = Vec::new();
        for token in tokens {
            match token.as_str() {
                "NOT" => ops.push(Operator::Not),
                "AND" => Self::handle_and(&mut ops, &mut out)?,
                "OR" => Self::handle_or(&mut ops, &mut out)?,
                "(" => ops.push(Operator::Open),
                ")" => Self::handle_close_paren(&mut ops, &mut out)?,
                _ => Self::handle_default(&mut ops, &mut out, &token)?,
            }
        }
        while let Some(op) = ops.pop() {
            let Some(right) = out.pop() else {
                return Err(io_error!("Invalid query"));
            };
            let Some(left) = out.pop() else {
                return Err(io_error!("Invalid query"));
            };
            out.push(WhereClause::Tree(Box::new(left), op, Box::new(right)));
        }
        if out.len() > 2 {
            Err(io_error!("Invalid query"))
        } else if let Some(res) = out.pop() {
            Ok((res, keyword))
        } else {
            Err(io_error!("Invalid query"))
        }
    }

    fn handle_and(ops: &mut Vec<Operator>, out: &mut Vec<WhereClause>) -> std::io::Result<()> {
        if let Some(Operator::And) = ops.last() {
            ops.pop();
            let Some(right) = out.pop() else {
                return Err(io_error!("Invalid query"));
            };
            let Some(left) = out.pop() else {
                return Err(io_error!("Invalid query"));
            };
            out.push(WhereClause::Tree(
                Box::new(left),
                Operator::And,
                Box::new(right),
            ));
        }
        ops.push(Operator::And);
        Ok(())
    }

    fn handle_or(ops: &mut Vec<Operator>, out: &mut Vec<WhereClause>) -> std::io::Result<()> {
        if matches!(ops.last(), Some(Operator::Or | Operator::And)) {
            let Some(op) = ops.pop() else {
                return Err(io_error!("Invalid query"));
            };
            let Some(right) = out.pop() else {
                return Err(io_error!("Invalid query"));
            };
            let Some(left) = out.pop() else {
                return Err(io_error!("Invalid query"));
            };
            out.push(WhereClause::Tree(Box::new(left), op, Box::new(right)));
        }
        ops.push(Operator::Or);
        Ok(())
    }

    fn handle_close_paren(
        ops: &mut Vec<Operator>,
        out: &mut Vec<WhereClause>,
    ) -> std::io::Result<()> {
        while let Some(op) = ops.pop() {
            if matches!(op, Operator::Open) {
                break;
            }
            let Some(right) = out.pop() else {
                return Err(io_error!("Invalid query"));
            };
            let Some(left) = out.pop() else {
                return Err(io_error!("Invalid query"));
            };
            out.push(WhereClause::Tree(Box::new(left), op, Box::new(right)));
        }
        Ok(())
    }

    fn handle_default(
        ops: &mut Vec<Operator>,
        out: &mut Vec<WhereClause>,
        token: &str,
    ) -> std::io::Result<()> {
        let not = match ops.last() {
            Some(Operator::Not) => {
                ops.pop();
                true
            }
            _ => false,
        };
        let comp = WhereClause::get_comparator(token, not)?;
        out.push(WhereClause::Comp(comp));
        Ok(())
    }

    /// Generates a `Comparator` based on the given clause string and negation flag.
    ///
    /// # Parameters
    ///
    /// - `clause`: The string representing the comparison clause.
    /// - `not`: A boolean flag indicating if the comparison should be negated.
    ///
    /// # Returns
    ///
    /// Returns a `Comparator` representing the comparison operation.
    fn get_comparator(clause: &str, not: Not) -> std::io::Result<Comparator> {
        let parts = clause.split_whitespace().collect::<Vec<&str>>();
        let mut left = parts[0].to_string();
        let mut break_point = 0;
        if not && parts.len() == 1 {
            return Ok(Comparator::Equal(left, "NULL".to_string(), false));
        }
        if parts.len() < 3 {
            return Err(io_error!(format!(
                "The following condition is invalid: {clause}"
            )));
        }
        for (idx, part) in parts[1..].iter().enumerate() {
            if part == &"=" || part == &">" || part == &"<" || part == &">=" || part == &"<=" {
                break_point = idx + 1;
                break;
            }
            left += &(" ".to_owned() + part);
        }
        if clause.len() <= break_point + 1 {
            return Err(io_error!("Invalid syntax"));
        }
        let right = parts[break_point + 1..].join(" ");

        match *parts
            .get(break_point)
            .ok_or(io_error!("Invalid operator"))?
        {
            "=" => Ok(Comparator::Equal(left, right, not)),
            ">" => Ok(Comparator::GreaterThan(left, right, not)),
            "<" => Ok(Comparator::LessThan(left, right, not)),
            ">=" => Ok(Comparator::GreaterThanOrEqual(left, right, not)),
            "<=" => Ok(Comparator::LessThanOrEqual(left, right, not)),
            _ => Err(io_error!("Invalid operator")),
        }
    }

    /// Evaluates the `WHERE` clause against a given row.
    ///
    /// # Parameters
    ///
    /// - `row`: A reference to a `HashMap<String, String>` containing the data of a row.
    ///
    /// # Returns
    ///
    /// * Returns `true` if the row satisfies the `WHERE` clause, `Ok(false)` otherwise
    ///
    /// # Errors
    ///
    /// * Returns an `Error` if an error occurs during evaluation.
    pub(crate) fn eval(
        &self,
        row: &HashMap<String, String>,
        schema: &Schema,
    ) -> std::io::Result<bool> {
        match self {
            WhereClause::Comp(comp) => match comp {
                Comparator::Equal(val1, val2, _)
                | Comparator::GreaterThan(val1, val2, _)
                | Comparator::LessThan(val1, val2, _)
                | Comparator::GreaterThanOrEqual(val1, val2, _)
                | Comparator::LessThanOrEqual(val1, val2, _) => {
                    WhereClause::process(val1, val2, row, comp, schema)
                }
            },
            WhereClause::Tree(left, op, right) => {
                let l = left.eval(row, schema)?;
                let r = right.eval(row, schema)?;
                match op {
                    Operator::And => Ok(l && r),
                    Operator::Or => Ok(l || r),
                    _ => Err(io_error!("Invalid operator")),
                }
            }
        }
    }

    /// Processes the comparison between two values within the context of a row.
    ///
    /// # Parameters
    ///
    /// - `val1`: The first value to be compared.
    /// - `val2`: The second value to be compared.
    /// - `row`: A reference to a `CsvRow` containing the data to be used in the comparison.
    /// - `op`: The comparison operation to be performed.
    ///
    /// # Returns
    ///
    /// * Returns `true` if the comparison is successful, `Ok(false)` otherwise
    ///
    /// # Errors
    ///
    /// * Returns an `Error` if an error occurs during the comparison.
    fn process(
        val1: &str,
        val2: &str,
        row: &HashMap<String, String>,
        op: &Comparator,
        schema: &Schema,
    ) -> std::io::Result<bool> {
        let owned_value1 = val1.to_string();
        let value1 = row.get(val1).unwrap_or_else(|| &owned_value1);

        let owned_value2 = val2.to_string();
        let value2 = row.get(val2).unwrap_or_else(|| &owned_value2);

        if value1 == "NULL" || value2 == "NULL" {
            return Ok(value1 == value2);
        }

        let schema_type = schema
            .get_schema_type(val1)
            .or_else(|| schema.get_schema_type(val2))
            .ok_or(io_error!("No valid column provided"))?;
        let result = schema_type.cmp(value1, value2)?;
        match op {
            Comparator::Equal(_, _, not) => Ok(result.is_eq() != *not),
            Comparator::GreaterThan(_, _, not) => Ok(result.is_gt() != *not),
            Comparator::LessThan(_, _, not) => Ok(result.is_lt() != *not),
            Comparator::GreaterThanOrEqual(_, _, not) => Ok(result.is_ge() != *not),
            Comparator::LessThanOrEqual(_, _, not) => Ok(result.is_le() != *not),
        }
    }

    pub(crate) fn get_keys(&self) -> Vec<(String, String)> {
        match self {
            WhereClause::Comp(comp) => match comp {
                Comparator::Equal(col, val, _)
                | Comparator::GreaterThan(col, val, _)
                | Comparator::LessThan(col, val, _)
                | Comparator::GreaterThanOrEqual(col, val, _)
                | Comparator::LessThanOrEqual(col, val, _) => {
                    vec![(col.to_string(), val.to_string())]
                }
            },
            WhereClause::Tree(left, _, right) => {
                let mut keys = left.get_keys();
                keys.extend(right.get_keys());
                keys
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use db::{PrimaryKey, SchemaType};

    use super::*;
    fn get_columns() -> HashMap<String, SchemaType> {
        HashMap::from([
            ("age".to_string(), SchemaType::Int),
            ("name".to_string(), SchemaType::Text),
            ("salary".to_string(), SchemaType::Int),
            ("experience".to_string(), SchemaType::Int),
        ])
    }

    fn get_schema() -> Schema {
        Schema::new(
            get_columns(),
            PrimaryKey::new(vec!["name".to_string()], vec!["age".to_string()]),
        )
    }

    #[test]
    fn test_single_comparator() {
        let input = vec!["age".to_string(), ">".to_string(), "30".to_string()];
        let result = WhereClause::new(&input);

        match result {
            Ok((WhereClause::Comp(Comparator::GreaterThan(s1, i1, false)), None)) => {
                assert_eq!(s1, "age");
                assert_eq!(i1, "30");
            }
            _ => panic!("Test failed for single comparator"),
        }
    }

    #[test]
    fn test_single_comparator_with_string_values() {
        let input = vec!["name".to_string(), "=".to_string(), "Alice".to_string()];
        let result = WhereClause::new(&input);

        match result {
            Ok((WhereClause::Comp(Comparator::Equal(s1, s2, false)), None)) => {
                assert_eq!(s1, "name");
                assert_eq!(s2, "Alice");
            }
            _ => panic!("Test failed for single comparator with string values"),
        }
    }

    #[test]
    fn test_name_with_spaces() {
        let input = vec![
            "full name".to_string(),
            "=".to_string(),
            "Alice Smith".to_string(),
        ];
        let result = WhereClause::new(&input);

        match result {
            Ok((WhereClause::Comp(Comparator::Equal(s1, s2, false)), None)) => {
                assert_eq!(s1, "full name");
                assert_eq!(s2, "Alice Smith");
            }
            _ => panic!("Test failed for name with spaces"),
        }
    }

    #[test]
    fn test_and_operator() {
        let input = vec![
            "age".to_string(),
            ">".to_string(),
            "30".to_string(),
            "AND".to_string(),
            "salary".to_string(),
            ">".to_string(),
            "50000".to_string(),
        ];
        let result = WhereClause::new(&input);

        match result {
            Ok((WhereClause::Tree(left, Operator::And, right), None)) => {
                if let WhereClause::Comp(Comparator::GreaterThan(s1, i1, false)) = *left {
                    assert_eq!(s1, "age");
                    assert_eq!(i1, "30");
                } else {
                    panic!("Test failed for AND operator - left side");
                }

                if let WhereClause::Comp(Comparator::GreaterThan(s2, i2, false)) = *right {
                    assert_eq!(s2, "salary");
                    assert_eq!(i2, "50000");
                } else {
                    panic!("Test failed for AND operator - right side");
                }
            }
            _ => panic!("Test failed for AND operator"),
        }
    }

    #[test]
    fn test_or_operator() {
        let input = vec![
            "age".to_string(),
            ">".to_string(),
            "30".to_string(),
            "OR".to_string(),
            "salary".to_string(),
            "<".to_string(),
            "50000".to_string(),
        ];
        let result = WhereClause::new(&input);

        match result {
            Ok((WhereClause::Tree(left, Operator::Or, right), None)) => {
                if let WhereClause::Comp(Comparator::GreaterThan(s1, i1, false)) = *left {
                    assert_eq!(s1, "age");
                    assert_eq!(i1, "30");
                } else {
                    panic!("Test failed for OR operator - left side");
                }

                if let WhereClause::Comp(Comparator::LessThan(s2, i2, false)) = *right {
                    assert_eq!(s2, "salary");
                    assert_eq!(i2, "50000");
                } else {
                    panic!("Test failed for OR operator - right side");
                }
            }
            _ => panic!("Test failed for OR operator"),
        }
    }

    #[test]
    fn test_not_operator() {
        let input = vec![
            "NOT".to_string(),
            "age".to_string(),
            "=".to_string(),
            "30".to_string(),
        ];
        let result = WhereClause::new(&input);

        match result {
            Ok((WhereClause::Comp(Comparator::Equal(s1, i1, true)), None)) => {
                assert_eq!(s1, "age");
                assert_eq!(i1, "30");
            }
            _ => panic!("Test failed for NOT operator"),
        }
    }

    #[test]
    fn test_complex_expression() {
        let input = vec![
            "age".to_string(),
            ">".to_string(),
            "30".to_string(),
            "AND".to_string(),
            "(".to_string(),
            "salary".to_string(),
            ">".to_string(),
            "50000".to_string(),
            "OR".to_string(),
            "experience".to_string(),
            ">".to_string(),
            "5".to_string(),
            ")".to_string(),
        ];
        let result = WhereClause::new(&input);

        match result {
            Ok((WhereClause::Tree(left, Operator::And, right), None)) => {
                if let WhereClause::Comp(Comparator::GreaterThan(s1, i1, false)) = *left {
                    assert_eq!(s1, "age");
                    assert_eq!(i1, "30");
                } else {
                    panic!("Test failed for complex expression - left side");
                }

                if let WhereClause::Tree(inner_left, Operator::Or, inner_right) = *right {
                    if let WhereClause::Comp(Comparator::GreaterThan(s2, i2, false)) = *inner_left {
                        assert_eq!(s2, "salary");
                        assert_eq!(i2, "50000");
                    } else {
                        panic!("Test failed for complex expression - inner left side");
                    }

                    if let WhereClause::Comp(Comparator::GreaterThan(s3, i3, false)) = *inner_right
                    {
                        assert_eq!(s3, "experience");
                        assert_eq!(i3, "5");
                    } else {
                        panic!("Test failed for complex expression - inner right side");
                    }
                } else {
                    panic!("Test failed for complex expression - right side");
                }
            }
            _ => panic!("Test failed for complex expression"),
        }
    }

    #[test]
    fn test_where_clause_single_comparator_equal() {
        let parts = vec!["age".to_string(), "=".to_string(), "30".to_string()];
        let (where_clause, _) = WhereClause::new(&parts).expect("Failed to parse WHERE clause");

        let mut row = HashMap::new();
        row.insert("age".to_string(), "30".to_string());
        row.insert("name".to_string(), "Alice".to_string());
        let schema = get_schema();

        let result = where_clause.eval(&row, &schema).expect("Evaluation failed");
        assert_eq!(result, true);
    }

    #[test]
    fn test_where_clause_single_comparator_not_equal() {
        let parts = vec!["age".to_string(), "=".to_string(), "30".to_string()];
        let (where_clause, _) = WhereClause::new(&parts).expect("Failed to parse WHERE clause");

        let mut row = HashMap::new();
        row.insert("age".to_string(), "25".to_string());
        let schema = get_schema();

        let result = where_clause.eval(&row, &schema).expect("Evaluation failed");
        assert_eq!(result, false);
    }

    #[test]
    fn test_where_clause_greater_than() {
        let parts = vec!["age".to_string(), ">".to_string(), "30".to_string()];
        let (where_clause, _) = WhereClause::new(&parts).expect("Failed to parse WHERE clause");

        let mut row = HashMap::new();
        row.insert("age".to_string(), "35".to_string());
        let schema = get_schema();

        let result = where_clause.eval(&row, &schema).expect("Evaluation failed");
        assert_eq!(result, true);
    }

    #[test]
    fn test_where_clause_less_than() {
        let parts = vec!["age".to_string(), "<".to_string(), "30".to_string()];
        let (where_clause, _) = WhereClause::new(&parts).expect("Failed to parse WHERE clause");

        let mut row = HashMap::new();
        row.insert("age".to_string(), "25".to_string());
        let schema = get_schema();

        let result = where_clause.eval(&row, &schema).expect("Evaluation failed");
        assert!(result);
    }

    #[test]
    fn test_where_clause_and() {
        let parts = vec![
            "age".to_string(),
            ">".to_string(),
            "30".to_string(),
            "AND".to_string(),
            "name".to_string(),
            "=".to_string(),
            "Alice".to_string(),
        ];
        let (where_clause, _) = WhereClause::new(&parts).expect("Failed to parse WHERE clause");

        let mut row = HashMap::new();
        row.insert("age".to_string(), "35".to_string());
        row.insert("name".to_string(), "Alice".to_string());
        let schema = get_schema();

        let result = where_clause.eval(&row, &schema).expect("Evaluation failed");
        assert_eq!(result, true);
    }

    #[test]
    fn test_where_clause_or() {
        let parts = vec![
            "age".to_string(),
            ">".to_string(),
            "30".to_string(),
            "OR".to_string(),
            "name".to_string(),
            "=".to_string(),
            "Bob".to_string(),
        ];
        let (where_clause, _) = WhereClause::new(&parts).expect("Failed to parse WHERE clause");

        let mut row = HashMap::new();
        row.insert("age".to_string(), "25".to_string());
        row.insert("name".to_string(), "Bob".to_string());
        let schema = get_schema();

        let result = where_clause.eval(&row, &schema).expect("Evaluation failed");
        assert_eq!(result, true);
    }

    #[test]
    fn test_where_clause_not() {
        let parts = vec![
            "NOT".to_string(),
            "age".to_string(),
            "=".to_string(),
            "30".to_string(),
        ];
        let (where_clause, _) = WhereClause::new(&parts).expect("Failed to parse WHERE clause");

        let mut row = HashMap::new();
        row.insert("age".to_string(), "25".to_string());
        let schema = get_schema();

        let result = where_clause.eval(&row, &schema).expect("Evaluation failed");
        assert_eq!(result, true);
    }

    #[test]
    fn test_where_clause_not_with_null() {
        let parts = vec!["NOT".to_string(), "age".to_string()];
        let (where_clause, _) = WhereClause::new(&parts).expect("Failed to parse WHERE clause");

        let mut row = HashMap::new();
        row.insert("age".to_string(), "NULL".to_string());
        let schema = get_schema();

        let result = where_clause.eval(&row, &schema).expect("Evaluation failed");
        assert_eq!(result, true);
    }

    #[test]
    fn test_where_clause_parentheses() {
        let parts = vec![
            "(".to_string(),
            "age".to_string(),
            ">".to_string(),
            "30".to_string(),
            "AND".to_string(),
            "name".to_string(),
            "=".to_string(),
            "Alice".to_string(),
            ")".to_string(),
        ];
        let (where_clause, _) = WhereClause::new(&parts).expect("Failed to parse WHERE clause");

        let mut row = HashMap::new();
        row.insert("age".to_string(), "35".to_string());
        row.insert("name".to_string(), "Alice".to_string());
        let schema = get_schema();

        let result = where_clause.eval(&row, &schema).expect("Evaluation failed");
        assert_eq!(result, true);
    }
}
