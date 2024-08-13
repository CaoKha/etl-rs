use polars::{
    lazy::dsl::{col, concat_str, lit, when, Expr},
    prelude::NULL,
};

use crate::jdd::schema::Jdd;

pub fn col_code_naf_with_polars_expr() -> Expr {
    // Define a Polars expression to clean and transform the code_naf column
    when(
        col(Jdd::CodeNaf.as_str())
            .str()
            .replace(lit("[.\\-_,;]"), lit(""), false)
            .str()
            .extract(lit(r"^(\d{4})[a-zA-Z]$"), 1)
            .is_null(),
    )
    .then(lit(NULL))
    .otherwise(concat_str(
        [
            col(Jdd::CodeNaf.as_str())
                .str()
                .replace(lit("[.\\-_,;]"), lit(""), false)
                .str()
                .extract(lit(r"^(\d{4})[a-zA-Z]$"), 1),
            col(Jdd::CodeNaf.as_str())
                .str()
                .replace(lit("[.\\-_,;]"), lit(""), false)
                .str()
                .extract(lit(r"^\d{4}([a-zA-Z])$"), 1)
                .str()
                .to_uppercase(),
        ],
        "",
        true,
    ))
    .alias(Jdd::CodeNaf.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::{datatypes::AnyValue, df, lazy::frame::IntoLazy};

    #[test]
    // Sample function to test col_code_naf_with_polars_expr
    fn test_col_code_naf_with_polars_expr() {
        // Create a DataFrame with test data
        let df = df![
            Jdd::CodeNaf.as_str() => [
                Some("011;1Z"),
                Some("1234a"),
                Some("5678B"),
                Some("1234"),
                Some("5678"),
                Some("12-34")
            ]
        ]
        .expect("DataFrame created failed");

        // Apply the expression
        let result_df = df
            .clone()
            .lazy()
            .select(&[col_code_naf_with_polars_expr()])
            .collect()
            .expect("DataFrame collected failed");

        // Expected DataFrame
        let expected_df = df![
            Jdd::CodeNaf.as_str() => [
                Some("0111Z"),
                Some("1234A"), // Unchanged
                Some("5678B"), // Unchanged
                None,
                None,
                None
            ]
        ]
        .expect("DataFrame created failed");

        // Extract the Series for comparison
        let result_series = result_df
            .column(Jdd::CodeNaf.as_str())
            .expect("Result column not found");
        let expected_series = expected_df
            .column(Jdd::CodeNaf.as_str())
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
