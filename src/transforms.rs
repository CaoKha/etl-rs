use regex::Regex;
use std::borrow::Cow;

fn strip_accent(nom: &str) -> String {
    let mut result = nom.to_lowercase();
    result = result.replace(|c: char| "àáâãäå".contains(c), "a");
    result = result.replace(|c: char| "èéêë".contains(c), "e");
    result = result.replace(|c: char| "ìíîï".contains(c), "i");
    result = result.replace(|c: char| "òóôõö".contains(c), "o");
    result = result.replace(|c: char| "ùúûü".contains(c), "u");
    result = result.replace(|c: char| "ç".contains(c), "c");
    result = result.replace(|c: char| "ñ".contains(c), "n");
    result
}

fn replace_delimiters_inside_nom(nom: &str, pattern: &str, replacement: &str) -> Result<String, regex::Error> {
    let re = Regex::new(pattern)?;

    let nom = re.replace_all(nom, |caps: &regex::Captures| {
        if let (Some(start), Some(end)) = (caps.get(0).map(|m| m.start()), caps.get(0).map(|m| m.end())) {
            let before_is_space = start > 0 && nom.chars().nth(start - 1).map(|c| c.is_whitespace()).unwrap_or(false);
            let after_is_space = end < nom.len() && nom.chars().nth(end).map(|c| c.is_whitespace()).unwrap_or(false);

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
    let nom = re.replace_all(&nom, " ").trim().to_string();
    Ok(nom)
}

pub fn transform_nom(text: Option<&str>) -> Option<String> {
    if let Some(nom) = text {
        let nom = nom.trim();
        if nom.is_empty() {
            return None;
        }

        let nom = strip_accent(nom).to_uppercase();
        
        let re = Regex::new(r"^[^a-zA-ZÀ-ÿ\s]+|[^a-zA-ZÀ-ÿ\s]+$").ok()?;
        let nom = re.replace_all(&nom, "").to_string();
        
        let nom = replace_delimiters_inside_nom(&nom, r"//|_|/|&", "ET").ok()?;
        
        let re = Regex::new(r"[^a-zA-Z0-9À-ÿ\s\-\'’]").ok()?;
        let nom = re.replace_all(&nom, "").to_string();
        
        let nom = Regex::new(r"\-+")
            .ok()?
            .replace_all(&nom, " ")
            .to_string();
        let nom = Regex::new(r"\s+")
            .ok()?
            .replace_all(&nom, " ")
            .to_string();

        Some(nom)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transform_nom() {
        let test_cases = vec![
            (Some("Jean-Dupont//Smith"), Some("JEAN DUPONT ET SMITH".to_string())),
            (Some("Marie-Curie&Einstein"), Some("MARIE CURIE ET EINSTEIN".to_string())),
            (Some("N/A"), Some("N ET A".to_string())),
            (Some("O'Neil & Sons"), Some("O'NEIL ET SONS".to_string())),
            (Some("El Niño"), Some("EL NINO".to_string())),
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
