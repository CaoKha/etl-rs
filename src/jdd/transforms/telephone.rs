use std::collections::HashSet;

use polars::{error::PolarsResult, series::Series};

use super::utils::transform_string_series;


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

pub fn transform_col_telephone(series: &Series) -> PolarsResult<Option<Series>> {
    transform_string_series(series, transform_telephone)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
