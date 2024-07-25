use polars::{
    datatypes::StringChunked,
    lazy::dsl::{col, lit, Expr, GetOutput},
    series::IntoSeries,
};

use crate::jdd::schema::Jdd;

pub fn col_siren_with_polars_expr() -> Expr {
    col(Jdd::Siren.as_str())
        .str()
        .replace_all(lit(r"-|\s"), lit(""), false) // Remove dashes and spaces
        .map(
            |series| {
                let result = series
                    .str()?
                    .into_iter()
                    .map(|opt_text| {
                        opt_text.and_then(|text| {
                            let cleaned = text.to_string();
                            if cleaned.chars().all(char::is_numeric) && cleaned.len() == 9 {
                                Some(cleaned)
                            } else {
                                None
                            }
                        })
                    })
                    .collect::<StringChunked>();
                Ok(Some(result.into_series()))
            },
            GetOutput::same_type(),
        )
        .alias(Jdd::Siren.as_str())
}
