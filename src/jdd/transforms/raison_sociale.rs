use polars::{
    datatypes::StringChunked,
    error::PolarsResult,
    lazy::dsl::{col, Expr, GetOutput},
    series::{IntoSeries, Series},
};

use crate::jdd::schema::Jdd;

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

pub fn col_raison_sociale_with_polars_expr() -> Expr {
    let handle_quotes = col(Jdd::RaisonSociale.as_str()).map(
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

    handle_quotes.alias(Jdd::RaisonSociale.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
