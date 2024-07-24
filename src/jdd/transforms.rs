use polars::{
    datatypes::{DataType, StringChunked},
    error::PolarsResult,
    lazy::dsl::{col, functions::concat_str, lit, when, Expr, GetOutput},
    prelude::NULL,
    series::{IntoSeries, Series},
};

use regex::Regex;
use std::{borrow::Cow, collections::HashSet};

use crate::{
    config::{CIVILITE_MAP, SPECIAL_CIVILITIES},
    jdd::schema::Jdd,
};

pub enum Transform {
    Nom,
    Prenom,
    Civilite,
    Email,
    RaisonSociale,
    Telephone,
    // Add other variants as needed
}

// Start of string normalization functions
fn strip_accent(text: &str) -> String {
    text.chars()
        .map(|c| match c.to_lowercase().next().unwrap() {
            'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' => {
                if c.is_uppercase() {
                    'A'
                } else {
                    'a'
                }
            }
            'è' | 'é' | 'ê' | 'ë' => {
                if c.is_uppercase() {
                    'E'
                } else {
                    'e'
                }
            }
            'ì' | 'í' | 'î' | 'ï' => {
                if c.is_uppercase() {
                    'I'
                } else {
                    'i'
                }
            }
            'ò' | 'ó' | 'ô' | 'õ' | 'ö' => {
                if c.is_uppercase() {
                    'O'
                } else {
                    'o'
                }
            }
            'ù' | 'ú' | 'û' | 'ü' => {
                if c.is_uppercase() {
                    'U'
                } else {
                    'u'
                }
            }
            'ç' => {
                if c.is_uppercase() {
                    'C'
                } else {
                    'c'
                }
            }
            'ñ' => {
                if c.is_uppercase() {
                    'N'
                } else {
                    'n'
                }
            }
            _ => c,
        })
        .collect()
}
// End of string normalization functions

// Start of transformation functions
fn transform_nom(opt_text: Option<&str>) -> Option<String> {
    #[inline]
    fn replace_delimiters_inside_text(
        text: &str,
        pattern: &str,
        replacement: &str,
    ) -> Result<String, regex::Error> {
        let re = Regex::new(pattern)?;

        let text = re.replace_all(text, |caps: &regex::Captures| {
            if let (Some(start), Some(end)) =
                (caps.get(0).map(|m| m.start()), caps.get(0).map(|m| m.end()))
            {
                let before_is_space = start > 0
                    && text
                        .chars()
                        .nth(start - 1)
                        .map(|c| c.is_whitespace())
                        .unwrap_or(false);
                let after_is_space = end < text.len()
                    && text
                        .chars()
                        .nth(end)
                        .map(|c| c.is_whitespace())
                        .unwrap_or(false);

                if before_is_space && after_is_space {
                    Cow::Borrowed(replacement)
                } else {
                    Cow::Owned(format!(" {} ", replacement))
                }
            } else {
                Cow::Borrowed(replacement)
            }
        });

        let re = Regex::new(r"\s+")?;
        let text = re.replace_all(&text, " ").trim().to_string();
        Ok(text)
    }

    opt_text.and_then(|text| {
        let text = text.trim();
        if text.is_empty() {
            return None;
        }

        let text = strip_accent(text).to_uppercase();

        let re = Regex::new(r"^[^a-zA-ZÀ-ÿ\s]+|[^a-zA-ZÀ-ÿ\s]+$").ok()?;
        let text = re.replace_all(&text, "").to_string();

        let text = replace_delimiters_inside_text(&text, r"//|_|/|&", "ET").ok()?;

        let re = Regex::new(r"[^a-zA-Z0-9À-ÿ\s\-\'’]").ok()?;
        let text = re.replace_all(&text, "").to_string();

        let text = Regex::new(r"\-+").ok()?.replace_all(&text, " ").to_string();
        let text = Regex::new(r"\s+").ok()?.replace_all(&text, " ").to_string();

        Some(text)
    })
}

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

fn transform_civilite(opt_text: Option<&str>) -> Option<String> {
    opt_text.and_then(|text| {
        let text = strip_accent(text.trim()).to_uppercase().to_string();

        if SPECIAL_CIVILITIES.contains(&text.as_str()) {
            return None;
        }

        let re = Regex::new(r"[.,/&\\]").unwrap();
        let text = re.replace_all(&text, " ");

        let parts: Vec<&str> = text.split_whitespace().collect();
        let mut full_titles = vec![];

        for part in parts {
            if let Some(title_ref) = CIVILITE_MAP.get(part) {
                let title = title_ref.to_string();
                if !full_titles.contains(&title) {
                    full_titles.push(title.clone());
                }
            }
        }

        let mut result = vec![];

        if full_titles.contains(&"MONSIEUR".to_string()) {
            result.push("MONSIEUR".to_string());
        }

        if full_titles.contains(&"MADAME".to_string()) {
            result.push("MADAME".to_string());
        }

        if result.is_empty() {
            None
        } else {
            Some(result.join(" "))
        }
    })
}

fn transform_email(opt_email: Option<&str>) -> Option<String> {
    // Define a regex for valid email structure
    let email_re = Regex::new(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$").unwrap();

    opt_email.and_then(|email| {
        // Remove spaces and convert to uppercase
        let email = email.replace(' ', "").to_uppercase();
        // Remove accents and specific characters
        let email = strip_accent(&email)
            .replace(&['\'', '’', '&'][..], "")
            .replace("@.", "@");

        // Validate email structure
        if !email_re.is_match(&email) {
            return None;
        }

        // Split email to get the domain part
        let parts: Vec<&str> = email.split('@').collect();
        if parts.len() != 2 {
            return None;
        }

        let domain = parts[1];
        let domain_parts: Vec<&str> = domain.split('.').collect();
        if domain_parts.len() < 2 {
            return None;
        }

        // Check for at least 2 characters before the extension
        if domain_parts[domain_parts.len() - 2].len() < 2 {
            return None;
        }

        // Ensure the extension length is between 2 and 4 characters
        let extension_len = domain_parts.last().unwrap().len();
        if !(2..=4).contains(&extension_len) {
            return None;
        }

        // Remove hyphens in the domain
        let domain = domain.replace('-', "");

        // Reconstruct and return the transformed email
        Some(format!("{}@{}", parts[0], domain))
    })
}

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

pub fn transform_telephone(opt_phone_number: Option<&str>) -> Option<String> {
    #[inline]
    fn remove_non_digits(input: &str) -> String {
        input.chars().filter(|c| c.is_ascii_digit()).collect()
    }
    #[inline]
    fn is_paid_service(number: &str, prefixes: &HashSet<&str>) -> bool {
        prefixes.iter().any(|&prefix| number.starts_with(prefix))
    }
    opt_phone_number.and_then(|number| {
        let number = remove_non_digits(number.trim());
        let number = number.as_str();
        let paid_prefixes: HashSet<&str> = ["81", "82", "83", "87", "89"].iter().copied().collect();

        match number.len() {
            10 if number.starts_with('0') && !is_paid_service(&number[1..], &paid_prefixes) => {
                Some(format!(
                    "+33 {} {} {} {} {}",
                    &number[1..2],
                    &number[2..4],
                    &number[4..6],
                    &number[6..8],
                    &number[8..10]
                ))
            }
            11 if number.starts_with("33") && !is_paid_service(&number[2..], &paid_prefixes) => {
                Some(format!(
                    "+33 {} {} {} {} {}",
                    &number[2..3],
                    &number[3..5],
                    &number[5..7],
                    &number[7..9],
                    &number[9..11]
                ))
            }
            12 if number.starts_with("00") && !is_paid_service(&number[2..], &paid_prefixes) => {
                Some(format!(
                    "+{} {} {} {} {} {}",
                    &number[2..4],
                    &number[4..5],
                    &number[5..7],
                    &number[7..9],
                    &number[9..11],
                    &number[11..13]
                ))
            }
            12 if number.starts_with("+33") && !is_paid_service(&number[3..], &paid_prefixes) => {
                Some(format!(
                    "+33 {} {} {} {} {}",
                    &number[3..4],
                    &number[4..6],
                    &number[6..8],
                    &number[8..10],
                    &number[10..12]
                ))
            }
            12 if number.starts_with("330") && !is_paid_service(&number[3..], &paid_prefixes) => {
                Some(format!(
                    "+33 {} {} {} {} {}",
                    &number[3..4],
                    &number[4..6],
                    &number[6..8],
                    &number[8..10],
                    &number[10..12]
                ))
            }
            9 if !is_paid_service(number, &paid_prefixes) => Some(format!(
                "+33 {} {} {} {} {}",
                &number[0..1],
                &number[1..3],
                &number[3..5],
                &number[5..7],
                &number[7..9]
            )),
            _ => None,
        }
    })
}
// End of transformation functions

// Start of column transformation functions
pub fn transform_string_series<F>(series: &Series, transform_fn: F) -> PolarsResult<Option<Series>>
where
    F: Fn(Option<&str>) -> Option<String> + Send + Sync + 'static,
{
    let ca = series.str()?;
    let transformed = ca.into_iter().map(transform_fn).collect::<StringChunked>();
    Ok(Some(transformed.into_series()))
}

fn transform_col_nom(series: &Series) -> PolarsResult<Option<Series>> {
    transform_string_series(series, transform_nom)
}

fn transform_col_prenom(series: &Series) -> PolarsResult<Option<Series>> {
    transform_string_series(series, transform_prenom)
}

fn transform_col_civilite(series: &Series) -> PolarsResult<Option<Series>> {
    transform_string_series(series, transform_civilite)
}

fn transform_col_email(series: &Series) -> PolarsResult<Option<Series>> {
    transform_string_series(series, transform_email)
}

fn transform_col_raison_sociale(series: &Series) -> PolarsResult<Option<Series>> {
    transform_string_series(series, transform_raison_sociale)
}

fn transform_col_telephone(series: &Series) -> PolarsResult<Option<Series>> {
    transform_string_series(series, transform_telephone)
}

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
    .then(lit(NULL).alias(Jdd::CodeNaf.as_str()))
    .otherwise(
        concat_str(
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
        )
        .alias(Jdd::CodeNaf.as_str()),
    )
}
// End of column transformation functions

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

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
