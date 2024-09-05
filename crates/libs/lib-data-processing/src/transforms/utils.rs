use std::collections::HashMap;

use polars::{
    datatypes::StringChunked,
    error::PolarsResult,
    frame::DataFrame,
    prelude::NamedFrom,
    series::{IntoSeries, Series},
};
use serde::Serialize;
use serde_json::Value;

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

pub fn struct_to_dataframe<T>(input: &[T]) -> DataFrame
where
    T: Serialize,
{
    // Serialize structs to a vector of maps
    let mut vec_of_maps = Vec::new();
    for e in input.iter() {
        let json_value = serde_json::to_value(e).unwrap();
        if let Value::Object(map) = json_value {
            vec_of_maps.push(map);
        }
    }

    // Initialize column vectors
    let mut columns: HashMap<String, Vec<Option<String>>> = HashMap::new();

    // Populate columns from the vector of maps
    for map in vec_of_maps.iter() {
        for (key, value) in map.iter() {
            let column = columns.entry(key.clone()).or_default();
            match value {
                Value::String(s) => column.push(Some(s.clone())),
                Value::Null => column.push(None),
                Value::Number(s) => {
                    if let Some(num) = s.as_f64() {
                        let num_i32: i64 = num as i64;
                        column.push(Some(num_i32.to_string().clone()))
                    } else {
                        column.push(Some(s.to_string().clone()))
                    }
                }
                _ => unreachable!(), // assuming all fields are Option<String>
            }
        }
    }

    // Create DataFrame
    let mut df = DataFrame::default();
    for (key, values) in columns.into_iter() {
        let series = Series::new(&key, values);
        df.with_column(series).unwrap();
    }
    df
}
