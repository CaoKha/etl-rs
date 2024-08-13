use polars::{
    datatypes::DataType,
    error::PolarsResult,
    lazy::dsl::{col, Expr, GetOutput},
    series::Series,
};

use self::{
    civilite::transform_col_civilite, email::transform_col_email, nom::transform_col_nom,
    prenom::transform_col_prenom, raison_sociale::transform_col_raison_sociale,
    telephone::transform_col_telephone,
};

use super::{config::Transform, schema::Jdd};

pub mod ape;
pub mod civilite;
pub mod code_naf;
pub mod email;
pub mod libelle_naf;
pub mod nom;
pub mod prenom;
pub mod raison_sociale;
pub mod siren;
pub mod siret;
pub mod telephone;
pub mod utils;

fn get_transform_col_fn(transform: &Transform) -> impl Fn(&Series) -> PolarsResult<Option<Series>> {
    match transform {
        Transform::Nom => transform_col_nom,
        Transform::Prenom => transform_col_prenom,
        Transform::Civilite => transform_col_civilite,
        Transform::Email => transform_col_email,
        Transform::RaisonSociale => transform_col_raison_sociale,
        Transform::Telephone => transform_col_telephone,
    }
}

pub fn col_with_udf_expr(column: Jdd, transform: Transform) -> Expr {
    let transform_col_fn = get_transform_col_fn(&transform);
    let column_expr = col(column.as_str());
    column_expr.map(
        move |series: Series| transform_col_fn(&series),
        GetOutput::from_type(DataType::String),
    )
}
