use polars::{
    datatypes::StringChunked,
    lazy::dsl::{col, Expr, GetOutput},
    series::IntoSeries,
};

use crate::jdd::schema::Jdd;

pub fn col_siret_with_polars_expr() -> Expr {
    col(Jdd::Siret.as_str())
        .map(
            |series| {
                let result = series
                    .str()?
                    .into_iter()
                    .map(|opt_text| {
                        opt_text.and_then(|text| {
                            // Remove spaces, dots, and dashes
                            let cleaned: String = text.chars().filter(|c| c.is_numeric()).collect();

                            // Check if the cleaned string is exactly 14 digits
                            if cleaned.len() == 14 {
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
        .alias(Jdd::Siret.as_str())
}
