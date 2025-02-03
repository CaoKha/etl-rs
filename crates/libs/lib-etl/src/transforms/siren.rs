use crate::schemas::{jdd::Jdd, AsString};
use polars::{
    datatypes::StringChunked,
    lazy::dsl::{col, lit, Expr, GetOutput},
    prelude::IntoColumn,
};

pub fn col_siren_with_polars_expr() -> Expr {
    col(Jdd::Siren.as_str())
        .str()
        .replace_all(lit(r"-|\s"), lit(""), false) // Remove dashes and spaces
        .map(
            |series| {
                let result = series
                    .str()?
                    .into_iter()
                    .map(|opt_text| {
                        opt_text.and_then(|text| {
                            let cleaned = text.to_string();
                            if cleaned.chars().all(char::is_numeric) && cleaned.len() == 9 {
                                Some(cleaned)
                            } else {
                                None
                            }
                        })
                    })
                    .collect::<StringChunked>();
                Ok(Some(result.into_column()))
            },
            GetOutput::same_type(),
        )
        .alias(Jdd::Siren.as_str())
}

#[cfg(test)]
mod test {
    use super::*;
    use polars::{datatypes::AnyValue, df, lazy::frame::IntoLazy};

    #[test]
    fn test_col_siren_with_polars_expr() {
        // Create a DataFrame with test data
        let df = df![
            Jdd::Siren.as_str() => &[
                Some("732829320"),
                Some("732829320111"),
                None
            ]
        ]
        .expect("DataFrame creation failed");

        // Apply the expression
        let result_df = df
            .clone()
            .lazy()
            .select(&[col_siren_with_polars_expr()])
            .collect()
            .expect("DataFrame collection failed");

        println!("{:#?}", result_df.head(Some(7)));
        // Expected DataFrame
        let expected_df = df![
            Jdd::Siren.as_str() => &[
                Some("732829320"),
                None,
                None
            ]
        ]
        .expect("Expected DataFrame creation failed");

        // Extract the Series for comparison
        let result_series = result_df
            .column(Jdd::Siren.as_str())
            .expect("Result column not found")
            .as_materialized_series();
        let expected_series = expected_df
            .column(Jdd::Siren.as_str())
            .expect("Expected column not found")
            .as_materialized_series();

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
