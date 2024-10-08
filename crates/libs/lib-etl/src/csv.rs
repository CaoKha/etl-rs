use csv::ReaderBuilder;
use serde_json::{json, Map, Value};
use core::error::Error;

pub fn csv_to_json(file_path: &str) -> Result<Vec<Value>, Box<dyn Error>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b';')
        .comment(Some(b'#'))
        .from_path(file_path)
        .map_err(|e| format!("Failed to open CSV file: {}", e))?;

    let headers = reader.headers()?.clone();
    let mut json_objects = Vec::new();

    for result in reader.records() {
        let record = result?;
        let json_obj = csv_row_to_json_object(&headers, &record)?;
        json_objects.push(Value::Object(json_obj));
    }

    Ok(json_objects)
}

fn csv_row_to_json_object(
    headers: &csv::StringRecord,
    record: &csv::StringRecord,
) -> Result<Map<String, Value>, Box<dyn Error>> {
    let mut json_obj = Map::new();
    for (header, value) in headers.iter().zip(record.iter()) {
        let json_value = if value.is_empty() {
            Value::Null
        } else {
            json!(value)
        };
        json_obj.insert(header.to_string(), json_value);
    }
    Ok(json_obj)
}
