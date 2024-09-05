use crate::schemas::{hdd::Hdd, AsString, SchemasEnum};
use polars::lazy::dsl::{col, lit, Expr};

fn transform_col_pce_expr(col_pce: &str) -> Expr {
    col(col_pce)
        .str()
        .replace_all(lit(r"\D"), lit(""), false)
        .alias(col_pce)

}

pub fn col_pce_with_polars_expr(se: SchemasEnum) -> Expr {
    match se {
        SchemasEnum::Hdd => transform_col_pce_expr(Hdd::Pce.as_str()),
        SchemasEnum::Jdd => col("JDD_PCE does not exist")
    }
    // Clean the column by removing special characters
}
