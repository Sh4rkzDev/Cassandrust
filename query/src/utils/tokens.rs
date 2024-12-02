use shared::io_error;

use crate::models::query::KEYWORDS;

/// Tokenizes a list of string parts based on a custom breaking function and predefined SQL keywords.
///
/// This function processes a list of strings, breaking them into tokens based on the provided
/// `func_break` function and the presence of SQL keywords. It handles special cases like parentheses
/// and combines parts that belong together.
///
/// # Arguments
///
/// * `parts` - A slice of strings to be tokenized.
/// * `func_break` - A function that determines where to stop tokenizing based on the current string part.
///
/// # Errors
///
/// * Returns an `Error` if the function encounters an invalid syntax.
///
/// # Returns
///
/// * A tuple containing:
///   - `Vec<String>`: A vector of tokens.
///   - `Option<usize>`: An optional index of the keyword that triggered the break, if any.
///
/// # Example
///
/// ```rust
/// #[cfg(test)]
/// mod example {
///     use super::tokenize;
///
///     #[test]
///     fn example_tokenize() {
///         let parts = vec!["SELECT".to_string(), "name".to_string(), "FROM".to_string(), "users".to_string()];
///         let (tokens, keyword_idx) = tokenize(&parts, |s| s == "FROM")?;
///         assert_eq!(tokens, vec!["SELECT", "name"]);
///         assert_eq!(keyword_idx, Some(2));
///     }
/// }
/// ```
pub fn tokenize<F>(parts: &[String], func_break: F) -> std::io::Result<(Vec<String>, Option<usize>)>
where
    F: Fn(&str) -> bool,
{
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut keyword = None;
    for (idx, part) in parts.iter().enumerate() {
        let part_upper = part.to_uppercase();
        let part_upper_str = part_upper.as_str();
        if KEYWORDS.contains(&part_upper_str) {
            if !token.is_empty() {
                tokens.push(token.trim().to_owned());
                token = String::new();
            }
            if func_break(part_upper_str) {
                keyword = Some(idx);
                break;
            }
            tokens.push(part_upper_str.to_owned());
            continue;
        }
        if part.starts_with('(') {
            let mut started = false;
            for c in part.chars() {
                if c == '(' {
                    if started {
                        return Err(io_error!(format!("Unexpected parenthesis inside '{part}'")));
                    }
                    tokens.push("(".to_string());
                } else if c == '\'' {
                    started = true;
                    continue;
                } else {
                    started = true;
                    token += &c.to_string();
                }
            }
        } else if part.ends_with(')') {
            token += " ";
            for c in part.chars() {
                if c == ')' {
                    let trimmed = token.trim();
                    if !trimmed.is_empty() {
                        tokens.push(trimmed.to_owned());
                    }
                    tokens.push(")".to_string());
                    token = String::new();
                } else {
                    token += &c.to_string();
                }
            }
        } else {
            token += &(" ".to_string() + part.replace('\'', "").as_str());
        }
    }
    if !token.is_empty() {
        tokens.push(token.trim().to_owned());
    }
    Ok((tokens, keyword))
}

/// Separates parentheses from parts of strings, splitting them into individual elements.
///
/// This function processes a vector of strings, isolating any leading or trailing parentheses into
/// separate elements in the returned vector. It's useful for cleaning up and organizing strings
/// that include parentheses.
///
/// # Arguments
///
/// * `parts` - A reference to a vector of strings where some elements may include parentheses.
///
/// # Returns
///
/// * `Vec<String>`: A new vector where any parentheses in the input strings have been separated
///   into their own elements.
///
/// # Errors
///
/// * Returns an `Error` if the number of closing parentheses exceeds the number of opening or vice versa.
///
/// # Example
///
/// ```rust
/// #[cfg(test)]
/// mod tests {
///     use super::separate_parenthesis;
///
///     #[test]
///     fn test_separate_parenthesis() {
///         let parts = vec!["(name".to_string(), "age)".to_string(), "location".to_string()];
///         let separated = separate_parenthesis(&parts).unwrap();
///         assert_eq!(separated, vec![
///          "(".to_string(), "name".to_string(), "age".to_string(),
///          ")".to_string(), "location".to_string()
///         ]);
///     }
/// }
/// ```
pub fn separate_parenthesis(parts: &Vec<String>) -> std::io::Result<Vec<String>> {
    let mut res = Vec::new();
    let mut open_parentheses = 0;
    let mut close_parentheses = 0;
    for part in parts {
        let mut current = part.as_str();

        while let Some(stripped) = current.strip_prefix('(') {
            res.push("(".to_string());
            open_parentheses += 1;
            current = stripped;
        }

        let mut stripped_part = current.to_string();
        let mut close_count = 0;
        while let Some(new_stripped) = stripped_part.strip_suffix(')') {
            close_count += 1;
            stripped_part = new_stripped.to_string();
        }

        if !stripped_part.is_empty() {
            res.push(stripped_part);
        }

        for _ in 0..close_count {
            res.push(")".to_string());
        }
        close_parentheses += close_count;
    }
    if open_parentheses != close_parentheses {
        return Err(io_error!("Parentheses mismatch"));
    }
    Ok(res)
}

pub fn get_columns_from_vec(s: &[String]) -> std::io::Result<Vec<String>> {
    let mut res = Vec::new();
    let mut token = String::new();
    for part in s {
        let part = part.replace('\'', "");
        if KEYWORDS.contains(&part.to_uppercase().as_str())
            || part == "="
            || part == ">"
            || part == "<"
            || part == "<="
            || part == ">="
            || part == "("
            || part == ")"
        {
            return Err(io_error!("Invalid column name"));
        }
        if let Some(stripped) = part.strip_suffix(',') {
            if stripped.contains(',') {
                return Err(io_error!("Invalid column name"));
            }
            token += &(" ".to_string() + stripped);
            res.push(token.trim().to_owned());
            token = String::new();
        } else if let Some(stripped) = part.strip_prefix(',') {
            if stripped.contains(',') {
                return Err(io_error!("Invalid column name"));
            }
            res.push(token.trim().to_owned());
            token = stripped.to_string();
        } else {
            token += &(" ".to_string() + &part);
        }
    }
    if !token.is_empty() {
        res.push(token.trim().to_owned());
    }
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_simple() -> std::io::Result<()> {
        let parts = vec![
            "SELECT".to_string(),
            "name".to_string(),
            "FROM".to_string(),
            "users".to_string(),
        ];
        let (tokens, keyword_idx) = tokenize(&parts, |s| s == "FROM")?;
        assert_eq!(tokens, vec!["SELECT", "name"]);
        assert_eq!(keyword_idx, Some(2));
        Ok(())
    }

    #[test]
    fn test_tokenize_with_parentheses() -> std::io::Result<()> {
        let parts = vec![
            "SELECT".to_string(),
            "(name".to_string(),
            "age)".to_string(),
            "FROM".to_string(),
            "users".to_string(),
        ];
        let (tokens, keyword_idx) = tokenize(&parts, |s| s == "FROM")?;
        assert_eq!(tokens, vec!["SELECT", "(", "name age", ")"]);
        assert_eq!(keyword_idx, Some(3));
        Ok(())
    }

    #[test]
    fn test_tokenize_no_keyword() -> std::io::Result<()> {
        let parts = vec!["SELECT".to_string(), "name".to_string(), "age".to_string()];
        let (tokens, keyword_idx) = tokenize(&parts, |s| s == "FROM")?;
        assert_eq!(tokens, vec!["SELECT", "name age"]);
        assert_eq!(keyword_idx, None);

        Ok(())
    }

    #[test]
    fn test_tokenize_with_keyword_break() -> std::io::Result<()> {
        let parts = vec![
            "SELECT".to_string(),
            "name".to_string(),
            "FROM".to_string(),
            "users".to_string(),
            "WHERE".to_string(),
        ];
        let (tokens, keyword_idx) = tokenize(&parts, |s| s == "WHERE")?;
        assert_eq!(tokens, vec!["SELECT", "name", "FROM", "users"]);
        assert_eq!(keyword_idx, Some(4));
        Ok(())
    }

    #[test]
    fn test_tokenize_with_single_quotes() -> std::io::Result<()> {
        let parts = vec![
            "SELECT".to_string(),
            "'full name'".to_string(),
            "age".to_string(),
            "FROM".to_string(),
            "users".to_string(),
        ];
        let (tokens, keyword_idx) = tokenize(&parts, |s| s == "FROM")?;
        assert_eq!(tokens, vec!["SELECT", "full name age"]);
        assert_eq!(keyword_idx, Some(3));
        Ok(())
    }

    #[test]
    fn test_tokenize_simulating_where_clause() -> std::io::Result<()> {
        let parts = vec![
            "age".to_string(),
            ">".to_string(),
            "30".to_string(),
            "AND".to_string(),
            "full name".to_string(),
            "=".to_string(),
            "'John Doe'".to_string(),
            "OR".to_string(),
            "email address".to_string(),
            "=".to_string(),
            "'leo@messi.goat'".to_string(),
        ];
        let (tokens, _) = tokenize(&parts, |s| s == "WHERE")?;
        assert_eq!(
            tokens,
            vec![
                "age > 30",
                "AND",
                "full name = John Doe",
                "OR",
                "email address = leo@messi.goat"
            ]
        );
        Ok(())
    }

    #[test]
    fn test_tokenize_simulating_where_with_two_columns() -> std::io::Result<()> {
        let parts = vec![
            "first name".to_string(),
            "=".to_string(),
            "second name".to_string(),
        ];
        let (tokens, _) = tokenize(&parts, |_| false)?;
        assert_eq!(tokens, vec!["first name = second name"]);
        Ok(())
    }

    #[test]
    fn test_separate_parenthesis_simple() -> std::io::Result<()> {
        let parts = vec![
            "(name".to_string(),
            "age)".to_string(),
            "location".to_string(),
        ];
        let separated = separate_parenthesis(&parts)?;
        assert_eq!(
            separated,
            vec![
                "(".to_string(),
                "name".to_string(),
                "age".to_string(),
                ")".to_string(),
                "location".to_string()
            ]
        );
        Ok(())
    }

    #[test]
    fn test_separate_parenthesis_no_parentheses() -> std::io::Result<()> {
        let parts = vec![
            "name".to_string(),
            "age".to_string(),
            "location".to_string(),
        ];
        let separated = separate_parenthesis(&parts)?;
        assert_eq!(
            separated,
            vec![
                "name".to_string(),
                "age".to_string(),
                "location".to_string()
            ]
        );
        Ok(())
    }

    #[test]
    fn test_separate_parenthesis_only_parentheses() -> std::io::Result<()> {
        let parts = vec!["(".to_string(), ")".to_string()];
        let separated = separate_parenthesis(&parts)?;
        assert_eq!(separated, vec!["(".to_string(), ")".to_string()]);
        Ok(())
    }

    #[test]
    fn test_balanced_parentheses() {
        let parts = vec!["(name".to_string(), "age".to_string(), ")".to_string()];
        let res = separate_parenthesis(&parts);
        assert!(res.is_ok());
    }

    #[test]
    fn test_imbalanced_parentheses() {
        let parts = vec!["(name".to_string(), "age".to_string()];
        let res = separate_parenthesis(&parts);
        assert!(res.is_err());
    }

    #[test]
    fn test_get_columns_from_vec_simple() -> std::io::Result<()> {
        let parts = vec!["name,".to_string(), "age".to_string()];
        let res = get_columns_from_vec(&parts)?;
        assert_eq!(res, vec!["name", "age"]);
        Ok(())
    }

    #[test]
    fn test_get_columns_from_vec_with_spaces() -> std::io::Result<()> {
        let parts = vec![
            "full".to_string(),
            "name,".to_string(),
            " age,".to_string(),
            " location".to_string(),
        ];
        let res = get_columns_from_vec(&parts)?;
        assert_eq!(res, vec!["full name", "age", "location"]);
        Ok(())
    }

    #[test]
    fn test_get_columns_with_quotes() -> std::io::Result<()> {
        let parts = vec![
            "'full".to_string(),
            "name',".to_string(),
            "'age',".to_string(),
            "location".to_string(),
        ];
        let res = get_columns_from_vec(&parts)?;
        assert_eq!(res, vec!["full name", "age", "location"]);
        Ok(())
    }

    #[test]
    fn test_multiple_contiguous_parenthesis() {
        let parts = vec!["(hello".to_string(), "(world))".to_string()];
        let res = separate_parenthesis(&parts);
        assert!(res.is_ok());
        assert_eq!(
            res.unwrap(),
            vec![
                "(".to_string(),
                "hello".to_string(),
                "(".to_string(),
                "world".to_string(),
                ")".to_string(),
                ")".to_string()
            ]
        )
    }
}
