use crate::config::{CIVILITE_MAP, SPECIAL_CIVILITIES};
use polars::{error::PolarsResult, prelude::Column};
use regex::Regex;

use super::utils::{strip_accent, transform_string_series};

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

pub fn transform_col_civilite(col: &Column) -> PolarsResult<Option<Column>> {
    transform_string_series(col, transform_civilite)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
