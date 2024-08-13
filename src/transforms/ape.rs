use crate::schemas::jdd::Jdd;
use polars::{
    lazy::dsl::{col, concat_str, lit, when, Expr},
    prelude::NULL,
};

use super::AsString;

pub fn col_ape_with_polars_expr() -> Expr {
    // Clean the column by removing special characters
    let clean_col = col(Jdd::Ape.as_str())
        .str()
        .replace_all(lit(r"[.\-_,]"), lit(""), false);

    // Extract the first four digits
    let extracted_digits = clean_col
        .clone()
        .str()
        .extract(lit(r"^(\d{4})[a-zA-Z]?$"), 1);
    // Extract the optional letter at the end and convert to uppercase
    let extracted_letter = clean_col
        .clone()
        .str()
        .extract(lit(r"^\d{4}([a-zA-Z])$"), 1)
        .str()
        .to_uppercase();

    // Create the new column based on the conditions
    when(
        clean_col
            .str()
            .extract(lit(r"^(\d{4})[a-zA-Z]$"), 1)
            .is_null(),
    )
    .then(lit(NULL))
    .otherwise(concat_str([extracted_digits, extracted_letter], "", true))
    .alias(Jdd::Ape.as_str())
}

#[cfg(test)]
mod test {
    use super::*;
    use polars::{datatypes::AnyValue, df, lazy::frame::IntoLazy};

    #[test]
    fn test_col_ape_with_polars_expr() {
        // Create a DataFrame with test data
        let df = df![
            Jdd::Ape.as_str() => &[
                Some("62.01z"),
                Some("62,01z"),
                Some("94z"),
                Some("12325"),
                Some("a2325"),
                None
            ]
        ]
        .expect("DataFrame creation failed");

        // Apply the expression
        let result_df = df
            .clone()
            .lazy()
            .select(&[col_ape_with_polars_expr()])
            .collect()
            .expect("DataFrame collection failed");

        println!("{:#?}", result_df.head(Some(7)));
        // Expected DataFrame
        let expected_df = df![
            Jdd::Ape.as_str() => &[
                Some("6201Z"),
                Some("6201Z"),
                None,
                None,
                None,
                None
            ]
        ]
        .expect("Expected DataFrame creation failed");

        // Extract the Series for comparison
        let result_series = result_df
            .column(Jdd::Ape.as_str())
            .expect("Result column not found");
        let expected_series = expected_df
            .column(Jdd::Ape.as_str())
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
