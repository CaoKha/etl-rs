use crate::schemas::{hdd::Hdd, jdd::Jdd, AsString, SchemasEnum};
use polars::{
    datatypes::StringChunked,
    lazy::dsl::{col, Expr, GetOutput},
    prelude::IntoColumn,
};

fn transform_col_siret_expr(col_siret: &str) -> Expr {
    col(col_siret)
        .map(
            |series| {
                let result = series
                    .str()?
                    .into_iter()
                    .map(|opt_text| {
                        opt_text.and_then(|text| {
                            // Remove spaces, dots, and dashes
                            let cleaned: String = text.chars().filter(|c| c.is_numeric()).collect();

                            // Check if the cleaned string is exactly 14 digits
                            if cleaned.len() == 14 {
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
        .alias(col_siret)
}

pub fn col_siret_with_polars_expr(se: SchemasEnum) -> Expr {
    match se {
        SchemasEnum::Jdd => transform_col_siret_expr(Jdd::Siret.as_str()),
        SchemasEnum::Hdd => transform_col_siret_expr(Hdd::Siret.as_str()),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::schemas::AsString;
    use polars::{datatypes::AnyValue, df, lazy::frame::IntoLazy};

    #[test]
    fn test_col_siret_with_polars_expr() {
        // Create a DataFrame with test data
        let df = df![
            Jdd::Siret.as_str() => &[
                Some("443 169 524 00120"),
                Some("443.169.524.00120"),
                Some("443 169 524 GH780"),
                Some("4ZT 169 524 00120"),
                None
            ]
        ]
        .expect("DataFrame creation failed");

        // Apply the expression
        let result_df = df
            .clone()
            .lazy()
            .select(&[col_siret_with_polars_expr(SchemasEnum::Jdd)])
            .collect()
            .expect("DataFrame collection failed");

        println!("{:#?}", result_df.head(Some(7)));
        // Expected DataFrame
        let expected_df = df![
            Jdd::Siret.as_str() => &[
                Some("44316952400120"),
                Some("44316952400120"),
                None,
                None,
                None
            ]
        ]
        .expect("Expected DataFrame creation failed");

        // Extract the Series for comparison
        let result_series = result_df
            .column(Jdd::Siret.as_str())
            .expect("Result column not found")
            .as_materialized_series();
        let expected_series = expected_df
            .column(Jdd::Siret.as_str())
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
