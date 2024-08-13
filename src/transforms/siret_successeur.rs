use crate::schemas::{hdd::Hdd, AsString, SchemasEnum};
use polars::lazy::dsl::{col, lit, Expr};

fn transform_col_siret_ss_expr(col_pce: &str) -> Expr {
    col(col_pce)
        .str()
        .replace_all(lit(r"\D"), lit(""), false)
        .alias(col_pce)

}

pub fn col_siret_ss_with_polars_expr(se: SchemasEnum) -> Expr {
    match se {
        SchemasEnum::Hdd => transform_col_siret_ss_expr(Hdd::SiretSuccesseur.as_str()),
        SchemasEnum::Jdd => col("JDD_SIRET_SUCCESSEUR does not exist")
    }
    // Clean the column by removing special characters
}
