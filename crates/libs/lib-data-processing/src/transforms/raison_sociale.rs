use polars::{
    datatypes::StringChunked,
    error::PolarsResult,
    lazy::dsl::{col, Expr, GetOutput},
    series::{IntoSeries, Series},
};

use crate::schemas::{hdd::Hdd, jdd::Jdd, AsString, SchemasEnum};

use super::utils::{strip_accent, transform_string_series};

fn transform_raison_sociale(opt_text: Option<&str>) -> Option<String> {
    opt_text.map(|text| {
        // Remove accents by replacing characters
        let text = strip_accent(text);
        // Handle quotes at the beginning and end of the string
        let text = if text.starts_with('"') && text.ends_with('"') {
            &text[1..text.len() - 1]
        } else {
            &text
        };

        // Replace double quotes with a single quote
        let text = text.replace("\"\"", "\"");

        // Convert to uppercase and handle special characters
        let text: String = text
            .chars()
            .map(|c| match c {
                'ß' => "ß".to_string(),
                _ => c.to_uppercase().to_string(),
            })
            .collect();

        Some(text)
    })?
}

pub fn transform_col_raison_sociale(series: &Series) -> PolarsResult<Option<Series>> {
    transform_string_series(series, transform_raison_sociale)
}

fn transform_col_raison_sociale_expr(col_rs: &str) -> Expr {
    let handle_quotes = col(col_rs).map(
        |series| {
            let result = series
                .str()?
                .into_iter()
                .map(|opt_text| {
                    opt_text.map(|text| {
                        let text = strip_accent(text);
                        let text = if text.starts_with('"') && text.ends_with('"') {
                            &text[1..text.len() - 1]
                        } else {
                            &text
                        };
                        let text = text.replace("\"\"", "\"");
                        text.chars()
                            .map(|c| {
                                if c == 'ß' {
                                    c.to_string()
                                } else {
                                    c.to_uppercase().to_string()
                                }
                            })
                            .collect::<String>()
                    })
                })
                .collect::<StringChunked>();
            Ok(Some(result.into_series()))
        },
        GetOutput::same_type(),
    );

    handle_quotes.alias(col_rs)
}

pub fn col_raison_sociale_with_polars_expr(se: SchemasEnum) -> Expr {
    match se {
        SchemasEnum::Jdd => transform_col_raison_sociale_expr(Jdd::RaisonSociale.as_str()),
        SchemasEnum::Hdd => transform_col_raison_sociale_expr(Hdd::RaisonSociale.as_str()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemas::AsString;
    use polars::{datatypes::AnyValue, df, lazy::frame::IntoLazy};

    #[test]
    fn test_transform_raison_sociale() {
        let test_cases = vec![
            (Some("\"ED\"\"BANGER\""), Some("ED\"BANGER".to_string())),
            // (Some("\"ED\"\"BANGER\"\"\"\""), Some("ED\"BANGER".to_string())), // Uncomment if needed
            (Some("Imagin&tiff_"), Some("IMAGIN&TIFF_".to_string())),
            (Some("S’ociété"), Some("S’OCIETE".to_string())),
            (Some("VECCHIA/"), Some("VECCHIA/".to_string())),
            (Some("//MONEYY//"), Some("//MONEYY//".to_string())),
            (Some("Straße"), Some("STRAßE".to_string())),
            (Some("Ve&ccio"), Some("VE&CCIO".to_string())),
            (Some("édouardservices"), Some("EDOUARDSERVICES".to_string())),
            (Some("imagin//"), Some("IMAGIN//".to_string())),
            (Some("HecøTOR"), Some("HECØTOR".to_string())),
            (Some("ed'GAR"), Some("ED'GAR".to_string())),
            (Some("Société dupont"), Some("SOCIETE DUPONT".to_string())),
            (Some("villiers"), Some("VILLIERS".to_string())),
            (Some("Paul&JO"), Some("PAUL&JO".to_string())),
            (Some("\"\"vanescènce\""), Some("\"VANESCENCE".to_string())),
            // (Some("\"\"\"\"\"\"\"\"vanescènce\""), Some("\"VANESCENCE".to_string())), // Uncomment if needed
            (Some("Brøgger"), Some("BRØGGER".to_string())),
            (Some("A"), Some("A".to_string())),
            (None, None),
            (Some("TIGER_Milk"), Some("TIGER_MILK".to_string())),
            (Some("漢字"), Some("漢字".to_string())),
        ];

        for (input, expected) in test_cases {
            let result = transform_raison_sociale(input);
            assert_eq!(result, expected, "Failed on input: {:?}", input);
        }
    }

    #[test]
    fn test_col_raison_sociale_with_polars_expr() {
        // Create a DataFrame with test data
        let df = df![
            Jdd::RaisonSociale.as_str() => &[
            Some("\"ED\"\"BANGER\""),
            Some("Imagin&tiff_"),
            Some("S’ociété"),
            Some("VECCHIA/"),
            Some("//MONEYY//"),
            Some("Straße"),
            Some("Ve&ccio"),
            Some("édouardservices"),
            Some("imagin//"),
            Some("HecøTOR"),
            Some("ed'GAR"),
            Some("Société dupont"),
            Some("villiers"),
            Some("Paul&JO"),
            Some("\"\"vanescènce\""),
            Some("Brøgger"),
            Some("A"),
            None,
            Some("TIGER_Milk"),
            Some("漢字"),
            ]
        ]
        .expect("DataFrame creation failed");

        // Apply the expression
        let result_df = df
            .clone()
            .lazy()
            .select(&[col_raison_sociale_with_polars_expr(SchemasEnum::Jdd)])
            .collect()
            .expect("DataFrame collection failed");

        println!("{:#?}", result_df.head(Some(7)));
        // Expected DataFrame
        let expected_df = df![
            Jdd::RaisonSociale.as_str() => &[
            Some("ED\"BANGER"),
            Some("IMAGIN&TIFF_"),
            Some("S’OCIETE"),
            Some("VECCHIA/"),
            Some("//MONEYY//"),
            Some("STRAßE"),
            Some("VE&CCIO"),
            Some("EDOUARDSERVICES"),
            Some("IMAGIN//"),
            Some("HECØTOR"),
            Some("ED'GAR"),
            Some("SOCIETE DUPONT"),
            Some("VILLIERS"),
            Some("PAUL&JO"),
            Some("\"VANESCENCE"),
            Some("BRØGGER"),
            Some("A"),
            None,
            Some("TIGER_MILK"),
            Some("漢字"),
            ]
        ]
        .expect("Expected DataFrame creation failed");

        // Extract the Series for comparison
        let result_series = result_df
            .column(Jdd::RaisonSociale.as_str())
            .expect("Result column not found");
        let expected_series = expected_df
            .column(Jdd::RaisonSociale.as_str())
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
