use polars::{
    lazy::dsl::{col, concat_str, lit, when, Expr},
    prelude::NULL,
};

use crate::jdd::schema::Jdd;

pub fn col_libelle_naf_with_polars_expr() -> Expr {
    let clean_col_expr =
        col(Jdd::LibeleNaf.as_str())
            .str()
            .replace(lit("[.\\-_,;]"), lit(""), false);
    // Define a Polars expression to clean and transform the code_naf column
    when(
        clean_col_expr
            .clone()
            .str()
            .extract(lit(r"^(\d{4})[a-zA-Z]$"), 1)
            .is_null(),
    )
    .then(lit(NULL))
    .otherwise(concat_str(
        [
            clean_col_expr
                .clone()
                .str()
                .extract(lit(r"^(\d{4})[a-zA-Z]$"), 1),
            clean_col_expr
                .str()
                .extract(lit(r"^\d{4}([a-zA-Z])$"), 1)
                .str()
                .to_uppercase(),
        ],
        "",
        true,
    ))
    .alias(Jdd::LibeleNaf.as_str())
}
