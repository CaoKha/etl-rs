use polars::{
    lazy::dsl::{col, concat_str, lit, when, Expr},
    prelude::NULL,
};

use crate::jdd::schema::Jdd;

pub fn col_code_naf_with_polars_expr() -> Expr {
    // Define a Polars expression to clean and transform the code_naf column
    when(
        col(Jdd::CodeNaf.as_str())
            .str()
            .replace(lit("[.\\-_,;]"), lit(""), false)
            .str()
            .extract(lit(r"^(\d{4})[a-zA-Z]$"), 1)
            .is_null(),
    )
    .then(lit(NULL))
    .otherwise(concat_str(
        [
            col(Jdd::CodeNaf.as_str())
                .str()
                .replace(lit("[.\\-_,;]"), lit(""), false)
                .str()
                .extract(lit(r"^(\d{4})[a-zA-Z]$"), 1),
            col(Jdd::CodeNaf.as_str())
                .str()
                .replace(lit("[.\\-_,;]"), lit(""), false)
                .str()
                .extract(lit(r"^\d{4}([a-zA-Z])$"), 1)
                .str()
                .to_uppercase(),
        ],
        "",
        true,
    ))
    .alias(Jdd::CodeNaf.as_str())
}
