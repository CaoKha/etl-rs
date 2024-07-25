use std::borrow::Cow;

use polars::datatypes::StringChunked;
use polars::lazy::dsl::{col, lit, Expr, GetOutput};
use polars::series::IntoSeries;
use polars::{error::PolarsResult, series::Series};
use regex::Regex;

use crate::jdd::schema::Jdd;

use super::utils::strip_accent;

use super::utils::transform_string_series;

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

pub fn transform_col_nom(series: &Series) -> PolarsResult<Option<Series>> {
    transform_string_series(series, transform_nom)
}

pub fn col_nom_with_polars_expr() -> Expr {
    col(Jdd::Nom.as_str())
        .str()
        .replace_all(lit(r"^\s+|\s+$"), lit(""), false) // Trim spaces
        .map(
            |series| {
                let s = series
                    .str()?
                    .into_iter()
                    .map(|opt_text| {
                        opt_text.map(|text| {
                            let text = strip_accent(text).to_uppercase();
                            Some(text)
                        })?
                    })
                    .collect::<StringChunked>();
                Ok(Some(s.into_series()))
            },
            GetOutput::same_type(),
        ) // Remove accents and convert to uppercase
        .str()
        .replace_all(lit(r"^[^a-zA-ZÀ-ÿ\s]+|[^a-zA-ZÀ-ÿ\s]+$"), lit(""), false) // Remove leading/trailing non-alphabetic chars
        .str()
        .replace(lit(r"//|_|/|&"), lit(" ET "), false) // Replace certain delimiters with "ET"
        .str()
        .replace_all(lit(r"[^a-zA-Z0-9À-ÿ\s\-\'’]"), lit(""), false) // Remove invalid characters
        .str()
        .replace_all(lit(r"\-+"), lit(" "), false) // Replace multiple hyphens with a single space
        .str()
        .replace_all(lit(r"\s+"), lit(" "), false) // Replace multiple spaces with a single space
        .alias(Jdd::Nom.as_str()) // Alias the output column name
}

#[cfg(test)]
mod tests {
    use crate::jdd::transforms::nom::transform_nom;

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
}
