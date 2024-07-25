use polars::{
    datatypes::{DataType, StringChunked},
    error::PolarsResult,
    lazy::dsl::{col, functions::concat_str, lit, when, Expr, GetOutput},
    prelude::NULL,
    series::{IntoSeries, Series},
};

use regex::Regex;
use std::{borrow::Cow, collections::HashSet};

use self::{civilite::transform_col_civilite, email::transform_col_email, nom::transform_col_nom, prenom::transform_col_prenom, raison_sociale::transform_col_raison_sociale, telephone::transform_col_telephone};

use super::{
    config::{Transform, CIVILITE_MAP, SPECIAL_CIVILITIES},
    schema::Jdd,
};

mod ape;
mod civilite;
mod code_naf;
mod email;
mod libelle_naf;
mod nom;
mod prenom;
mod raison_sociale;
mod siren;
mod siret;
mod telephone;
mod utils;

fn get_transform_col_fn(transform: &Transform) -> impl Fn(&Series) -> PolarsResult<Option<Series>> {
    match transform {
        Transform::Nom => transform_col_nom,
        Transform::Prenom => transform_col_prenom,
        Transform::Civilite => transform_col_civilite,
        Transform::Email => transform_col_email,
        Transform::RaisonSociale => transform_col_raison_sociale,
        Transform::Telephone => transform_col_telephone,
    }
}

pub fn col_with_udf_expr(column: Jdd, transform: Transform) -> Expr {
    let transform_col_fn = get_transform_col_fn(&transform);
    let column_expr = col(column.as_str());
    column_expr.map(
        move |series: Series| transform_col_fn(&series),
        GetOutput::from_type(DataType::String),
    )
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use polars::{datatypes::AnyValue, df, lazy::frame::IntoLazy};

    #[test]
    fn test_transform_nom() {
        let test_cases = vec![
            (
                Some("Jean-Dupont//Smith"),
                Some("JEAN DUPONT ET SMITH".to_string()),
            ),
            (
                Some("Marie-Curie&Einstein"),
                Some("MARIE CURIE ET EINSTEIN".to_string()),
            ),
            (Some("N/A"), Some("N ET A".to_string())),
            (Some("O'Neil & Sons"), Some("O'NEIL ET SONS".to_string())),
            (Some("El Niño"), Some("EL NINO".to_string())),
            (
                Some("&Carre & Lagrave&"),
                Some("CARRE ET LAGRAVE".to_string()),
            ),
            (
                Some("/Sébastien / Pascal/"),
                Some("SEBASTIEN ET PASCAL".to_string()),
            ),
            (Some("Carre_/"), Some("CARRE".to_string())),
            (Some("Brøgger"), Some("BRØGGER".to_string())),
            (None, None),
            (Some(""), None),
            (Some("    "), None),
        ];

        for (input, expected) in test_cases {
            let result = transform_nom(input);
            assert_eq!(result, expected, "Failed on input: {:?}", input);
        }
    }

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
    fn test_transform_civilite() {
        let test_cases = vec![
            (Some("Mm"), Some("MONSIEUR".to_string())),
            (Some("MR"), Some("MONSIEUR".to_string())),
            (Some("Ms"), Some("MADAME".to_string())),
            (Some("MMe"), Some("MADAME".to_string())),
            (Some("M(espace)"), Some("MONSIEUR".to_string())),
            (Some("MAD"), Some("MADAME".to_string())),
            (Some("MADAME"), Some("MADAME".to_string())),
            (Some("MM Mme"), Some("MONSIEUR MADAME".to_string())),
            (Some("Mme M."), Some("MONSIEUR MADAME".to_string())),
            (Some("MISS"), None),
            (None, None),
        ];

        for (input, expected) in test_cases {
            let result = transform_civilite(input);
            assert_eq!(result, expected, "Failed on input: {:?}", input);
        }
    }

    #[test]
    fn test_transform_email() {
        let test_cases = vec![
            (
                Some("Lucas31@gmail.com"),
                Some("LUCAS31@GMAIL.COM".to_string()),
            ),
            (
                Some("Lucas 31@gmail.com"),
                Some("LUCAS31@GMAIL.COM".to_string()),
            ),
            (
                Some("Lucàs31@gmail.com"),
                Some("LUCAS31@GMAIL.COM".to_string()),
            ),
            (
                Some("Luc’’as31@gmail.com"),
                Some("LUCAS31@GMAIL.COM".to_string()),
            ),
            (
                Some("Lucas31@gmail.com"),
                Some("LUCAS31@GMAIL.COM".to_string()),
            ),
            (Some("@gmail.com"), None),
            (Some("Lucas31gmail.com"), None),
            (Some("Lucas31@g.com"), None),
            (Some("Lucas31@siapartnersrue(XXXX....XXXX).com"), None),
            (Some("Lucas31@"), None),
            (Some("Lucas31@gmail.c-om"), None),
            (
                Some("Lucas31@.gmail.com"),
                Some("LUCAS31@GMAIL.COM".to_string()),
            ),
            (Some("Lucas31@gmail."), None),
            (Some("Lucas31@gmail..com"), None),
            (Some("Lucas31@gmail.f"), None),
            (Some("Lucas31@gmail.commmee"), None),
            (None, None),
            (Some("em&ms@gmail..com"), None),
        ];

        for (input, expected) in test_cases {
            let result = transform_email(input);
            assert_eq!(result, expected, "Failed on input: {:?}", input);
        }
    }

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
    fn test_transform_telephone() {
        let test_cases = vec![
            (
                Some("07 85 78 45 21b"),
                Some("+33 7 85 78 45 21".to_string()),
            ),
            (
                Some("06.58.96.32.47"),
                Some("+33 6 58 96 32 47".to_string()),
            ),
            (
                Some("06-58-96a32’47"),
                Some("+33 6 58 96 32 47".to_string()),
            ),
            (Some("443-73-421-00395"), None),
            (Some("\"06.\"\"é/940592\""), None),
            (Some("081 6 75 57 98"), None),
            (
                Some("085 6 75 57 98"),
                Some("+33 8 56 75 57 98".to_string()),
            ),
            (None, None),
        ];

        for (input, expected) in test_cases {
            let result = transform_telephone(input);
            assert_eq!(
                result, expected,
                "For input {:?}, expected {:?} but got {:?}",
                input, expected, result
            );
        }
    }

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

    #[test]
    fn test_col_email_with_polars_expr() {
        // Create a DataFrame with test data
        let df = df![
            Jdd::Email.as_str() => &[
                Some("Lucas31@gmail.com"),
                Some("Lucas 31@gmail.com"),
                Some("Lucàs31@gmail.com"),
                Some("Luc’’as31@gmail.com"),
                Some("Lucas31@gmail.com"),
                Some("@gmail.com"),
                Some("Lucas31gmail.com"),
                Some("Lucas31@g.com"),
                Some("Lucas31@siapartnersrue(XXXX....XXXX).com"),
                Some("Lucas31@"),
                Some("Lucas31@gmail.c-om"),
                Some("Lucas31@.gmail.com"),
                Some("Lucas31@gmail."),
                Some("Lucas31@gmail..com"),
                Some("Lucas31@gmail.f"),
                Some("Lucas31@gmail.commmee"),
                None,
                Some("em&ms@gmail..com")
            ]
        ]
        .expect("DataFrame creation failed");

        // Apply the expression
        let result_df = df
            .clone()
            .lazy()
            .select(&[col_email_with_polars_expr()])
            .collect()
            .expect("DataFrame collection failed");

        println!("{:#?}", result_df);
        // Expected DataFrame
        let expected_df = df![
            Jdd::Email.as_str() => &[
                Some("LUCAS31@GMAIL.COM"),
                Some("LUCAS31@GMAIL.COM"),
                Some("LUCAS31@GMAIL.COM"),
                Some("LUCAS31@GMAIL.COM"),
                Some("LUCAS31@GMAIL.COM"),
                None,
                None,
                None,
                None,
                None,
                None,
                Some("LUCAS31@GMAIL.COM"),
                None,
                None,
                None,
                None,
                None,
                None
            ]
        ]
        .expect("Expected DataFrame creation failed");

        // Extract the Series for comparison
        let result_series = result_df
            .column(Jdd::Email.as_str())
            .expect("Result column not found");
        let expected_series = expected_df
            .column(Jdd::Email.as_str())
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

    #[test]
    fn test_col_nom_with_polars_expr() {
        // Create a DataFrame with test data
        let df = df![
            Jdd::Nom.as_str() => &[
                Some("&Carre & Lagrave&"),
                Some("/Sébastien / Pascal/"),
                Some("Carre_/"),
                Some("Brøgger"),
                None,
            ]
        ]
        .expect("DataFrame creation failed");

        // Apply the expression
        let result_df = df
            .clone()
            .lazy()
            .select(&[col_nom_with_polars_expr()])
            .collect()
            .expect("DataFrame collection failed");

        println!("{:#?}", result_df);
        // Expected DataFrame
        let expected_df = df![
            Jdd::Nom.as_str() => &[
                Some("CARRE ET LAGRAVE"),
                Some("SEBASTIEN ET PASCAL"),
                Some("CARRE"),
                Some("BRØGGER"),
                None
            ]
        ]
        .expect("Expected DataFrame creation failed");

        // Extract the Series for comparison
        let result_series = result_df
            .column(Jdd::Nom.as_str())
            .expect("Result column not found");
        let expected_series = expected_df
            .column(Jdd::Nom.as_str())
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
            .select(&[col_prenom_with_polars_expr()])
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
            .select(&[col_raison_sociale_with_polars_expr()])
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
            .expect("Result column not found");
        let expected_series = expected_df
            .column(Jdd::Siren.as_str())
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
            .select(&[col_siret_with_polars_expr()])
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
            .expect("Result column not found");
        let expected_series = expected_df
            .column(Jdd::Siret.as_str())
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
