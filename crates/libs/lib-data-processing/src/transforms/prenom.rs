use crate::schemas::{hdd::Hdd, jdd::Jdd, AsString, SchemasEnum};
use polars::{
    datatypes::StringChunked,
    error::PolarsResult,
    series::{IntoSeries, Series},
};
use regex::Regex;

use super::utils::{strip_accent, transform_string_series};
use polars::lazy::dsl::{col, lit, Expr, GetOutput};

fn transform_prenom(opt_text: Option<&str>) -> Option<String> {
    #[inline]
    fn format_name_part(part: &str) -> String {
        let sub_parts: Vec<&str> = part.split_whitespace().collect();
        let formatted_sub_parts: Vec<String> = sub_parts
            .iter()
            .map(|&sub_part| {
                let mut chars = sub_part.chars();
                let first_char = strip_accent(
                    chars
                        .next()
                        .expect("Invalid character")
                        .to_uppercase()
                        .collect::<String>()
                        .as_str(),
                );
                let remaining_chars = chars.as_str().to_lowercase();
                format!("{}{}", first_char, remaining_chars)
            })
            .collect();
        formatted_sub_parts.join(" ")
    }

    #[inline]
    fn process_text(text: &str) -> String {
        let text = text.trim();
        let text = Regex::new(r"\s+")
            .unwrap()
            .replace_all(text, " ")
            .to_string();
        let parts: Vec<&str> = text.split('-').collect();
        let formatted_parts: Vec<String> = parts
            .iter()
            .map(|&part| {
                if part.trim().len() == 1 {
                    part.trim().to_string()
                } else {
                    format_name_part(part)
                }
            })
            .collect();
        formatted_parts.join("-")
    }

    #[inline]
    fn remove_special_characters(text: &str) -> String {
        let pattern = Regex::new(r"[^\u{00C0}-\u{00FF}a-zA-Z\s\-\'’&]").expect("Invalid pattern");
        let text = pattern.replace_all(text, "");
        let text = Regex::new(r"&+")
            .expect("Invalid pattern")
            .replace_all(&text, " ")
            .to_string();
        text
    }

    opt_text.and_then(|text| {
        if text.len() == 1 {
            let pattern = Regex::new(r"[^a-zA-Z\u{00C0}-\u{00FF}]").unwrap();
            let text = pattern.replace_all(text, "").to_string();
            if text.is_empty() {
                None
            } else {
                Some(text)
            }
        } else {
            let text = remove_special_characters(text);
            let text = process_text(&text);
            Some(text)
        }
    })
}

pub fn transform_col_prenom(series: &Series) -> PolarsResult<Option<Series>> {
    transform_string_series(series, transform_prenom)
}

fn transform_col_prenom_expr(col_prenom: &str) -> Expr {
    let re_whitespace = Regex::new(r"\s+").unwrap();
    let re_special_chars = Regex::new(r"[^\u{00C0}-\u{00FF}a-zA-Z\s\-\'’&]").unwrap();
    let re_ampersands = Regex::new(r"&+").unwrap();

    col(col_prenom)
        .str()
        .replace_all(lit(r"^\s+|\s+$"), lit(""), false) // Trim leading/trailing spaces
        .map(
            move |series| {
                let s = series
                    .str()?
                    .into_iter()
                    .map(|opt_text| {
                        opt_text.map(|text| {
                            let text = re_special_chars.replace_all(text, "");
                            let text = re_ampersands.replace_all(&text, " ");
                            let text = re_whitespace.replace_all(&text, " ").to_string();

                            // Process each part of the name separated by '-'
                            let formatted_text = text
                                .split('-')
                                .map(|part| {
                                    let formatted_part = part
                                        .split_whitespace()
                                        .map(|sub_part| {
                                            let mut chars = sub_part.chars();
                                            let first_char = chars
                                                .next()
                                                .map(|c| {
                                                    strip_accent(
                                                        c.to_uppercase()
                                                            .collect::<String>()
                                                            .as_str(),
                                                    )
                                                })
                                                .unwrap_or(String::from(""));
                                            let remaining_chars = chars.as_str().to_lowercase();
                                            format!("{}{}", first_char, remaining_chars)
                                        })
                                        .collect::<Vec<String>>()
                                        .join(" ");
                                    formatted_part
                                })
                                .collect::<Vec<_>>()
                                .join("-");
                            Some(formatted_text)
                        })?
                        // let text = strip_accent(text).to_uppercase();
                    })
                    .collect::<StringChunked>();
                Ok(Some(s.into_series()))
            },
            GetOutput::same_type(),
        ) // Apply transformations
        .alias(col_prenom) // Alias the output column name
}

pub fn col_prenom_with_polars_expr(se: SchemasEnum) -> Expr {
    match se {
        SchemasEnum::Jdd => transform_col_prenom_expr(Jdd::Prenom.as_str()),
        SchemasEnum::Hdd => transform_col_prenom_expr(Hdd::Prenom.as_str()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::{datatypes::AnyValue, df, lazy::frame::IntoLazy};

    #[test]
    fn test_transform_prenom() {
        let test_cases = vec![
            (Some("amélie"), Some("Amélie".to_string())),
            (Some("LOUCA"), Some("Louca".to_string())),
            (Some("H-an"), Some("H-An".to_string())),
            (Some("élie"), Some("Elie".to_string())),
            (Some("anne-marie"), Some("Anne-Marie".to_string())),
            (Some("anne marie"), Some("Anne Marie".to_string())),
            (Some("Hélène*3"), Some("Hélène".to_string())),
            (Some("Hélène&Adelin"), Some("Hélène Adelin".to_string())),
            (None, None),
        ];

        for (input, expected) in test_cases {
            let result = transform_prenom(input);
            assert_eq!(result, expected, "Failed on input: {:?}", input);
        }
    }

    #[test]
    fn test_col_prenom_with_polars_expr() {
        // Create a DataFrame with test data
        let df = df![
            Jdd::Prenom.as_str() => &[
            Some("amélie"),
            Some("LOUCA"),
            Some("H-an"),
            Some("élie"),
            Some("anne-marie"),
            Some("anne marie"),
            Some("Hélène*3"),
            Some("Hélène&Adelin"),
            None,
            ]
        ]
        .expect("DataFrame creation failed");

        // Apply the expression
        let result_df = df
            .clone()
            .lazy()
            .select(&[col_prenom_with_polars_expr(SchemasEnum::Jdd)])
            .collect()
            .expect("DataFrame collection failed");

        println!("{:#?}", result_df);
        // Expected DataFrame
        let expected_df = df![
            Jdd::Prenom.as_str() => &[
            Some("Amélie"),
            Some("Louca"),
            Some("H-An"),
            Some("Elie"),
            Some("Anne-Marie"),
            Some("Anne Marie"),
            Some("Hélène"),
            Some("Hélène Adelin"),
            None
            ]
        ]
        .expect("Expected DataFrame creation failed");

        // Extract the Series for comparison
        let result_series = result_df
            .column(Jdd::Prenom.as_str())
            .expect("Result column not found");
        let expected_series = expected_df
            .column(Jdd::Prenom.as_str())
            .expect("Expected column not found");

        // Ensure the lengths of both Series are the same
        assert_eq!(
            result_series.len(),
            expected_series.len(),
            "Series lengths do not match"
        );

        // Compare each element in the Series
        for (result_value, expected_value) in result_series.iter().zip(expected_series.iter()) {
            match (result_value.clone(), expected_value.clone()) {
                (AnyValue::String(result_str), AnyValue::String(expected_str)) => {
                    assert_eq!(result_str, expected_str, "Values do not match")
                }
                (AnyValue::Null, AnyValue::Null) => {} // Both are None, so they are equal
                _ => panic!(
                    "Mismatched value types: {:?} vs {:?}",
                    result_value, expected_value
                ),
            }
        }
    }
}
