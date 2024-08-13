use polars::{
    datatypes::StringChunked,
    error::PolarsResult,
    series::{IntoSeries, Series},
};

pub fn strip_accent(text: &str) -> String {
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

pub fn transform_string_series<F>(series: &Series, transform_fn: F) -> PolarsResult<Option<Series>>
where
    F: Fn(Option<&str>) -> Option<String> + Send + Sync + 'static,
{
    let ca = series.str()?;
    let transformed = ca.into_iter().map(transform_fn).collect::<StringChunked>();
    Ok(Some(transformed.into_series()))
}
