use polars::{
    lazy::dsl::{col, concat_str, lit, when, Expr},
    prelude::NULL,
};

use crate::jdd::schema::Jdd;

pub fn col_ape_with_polars_expr() -> Expr {
    // Clean the column by removing special characters
    let clean_col = col(Jdd::Ape.as_str())
        .str()
        .replace_all(lit(r"[.\-_,]"), lit(""), false);

    // Extract the first four digits
    let extracted_digits = clean_col
        .clone()
        .str()
        .extract(lit(r"^(\d{4})[a-zA-Z]?$"), 1);
    // Extract the optional letter at the end and convert to uppercase
    let extracted_letter = clean_col
        .clone()
        .str()
        .extract(lit(r"^\d{4}([a-zA-Z])$"), 1)
        .str()
        .to_uppercase();

    // Create the new column based on the conditions
    when(
        clean_col
            .str()
            .extract(lit(r"^(\d{4})[a-zA-Z]$"), 1)
            .is_null(),
    )
    .then(lit(NULL))
    .otherwise(concat_str([extracted_digits, extracted_letter], "", true))
    .alias(Jdd::Ape.as_str())
}
