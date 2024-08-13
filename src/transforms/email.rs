use polars::{
    datatypes::StringChunked,
    error::PolarsResult,
    lazy::dsl::{col, lit, Expr, GetOutput},
    series::{IntoSeries, Series},
};
use regex::Regex;

use crate::schemas::{hdd::Hdd, jdd::Jdd, AsString, SchemasEnum};

use super::utils::{strip_accent, transform_string_series};

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

pub fn transform_col_email(series: &Series) -> PolarsResult<Option<Series>> {
    transform_string_series(series, transform_email)
}

fn transform_col_email_expr(col_email: &str) -> Expr {
    let valid_email_re = r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$";

    col(col_email)
        .str()
        .to_uppercase() // Convert to uppercase
        .str()
        .replace(lit(r"['’&\s]+"), lit(""), false) // Remove specific characters
        .str()
        .replace(lit("@\\."), lit("@"), false) // Remove dot after '@'
        .map(
            move |series| {
                let email_re = Regex::new(valid_email_re).unwrap();
                let s = series
                    .str()?
                    .into_iter()
                    .map(|opt_email| {
                        opt_email.and_then(|email| {
                            let email = strip_accent(email);
                            // Validate email structure
                            if !email_re.is_match(email.as_str()) {
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
                    })
                    .collect::<StringChunked>();
                Ok(Some(s.into_series()))
            },
            GetOutput::same_type(),
        )
        .alias(col_email)
}

pub fn col_email_with_polars_expr(se: SchemasEnum) -> Expr {
    match se {
        SchemasEnum::Jdd => transform_col_email_expr(Jdd::Email.as_str()),
        SchemasEnum::Hdd => transform_col_email_expr(Hdd::Email.as_str()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::{datatypes::AnyValue, df, lazy::frame::IntoLazy};

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
            .select(&[col_email_with_polars_expr(SchemasEnum::Jdd)])
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
}
