use polars::{
    datatypes::StringChunked,
    error::PolarsResult,
    series::{IntoSeries, Series},
};
use regex::Regex;
use std::borrow::Cow;

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

fn remove_special_characters(text: &str) -> String {
    let pattern = Regex::new(r"[^\u{00C0}-\u{00FF}a-zA-Z\s\-\'’&]").expect("Invalid pattern");
    let text = pattern.replace_all(text, "");
    let text = Regex::new(r"&+")
        .expect("Invalid pattern")
        .replace_all(&text, " ")
        .to_string();
    text
}

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
// End of string normalization functions

// Start of transformation functions
fn transform_nom(opt_text: Option<&str>) -> Option<String> {
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

pub fn transform_col_nom(series: &Series) -> PolarsResult<Option<Series>> {
    transform_string_series(series, transform_nom)
}

pub fn transform_col_prenom(series: &Series) -> PolarsResult<Option<Series>> {
    transform_string_series(series, transform_prenom)
}
// End of column transformation functions

// Test functions
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
}
