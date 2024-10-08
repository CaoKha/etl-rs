use super::AsString;
use sea_query::Iden;

#[derive(Iden)]
pub enum Hdd {
    Table,
    Id,
    Pce,
    IdSource,
    RaisonSociale,
    Siret,
    SiretSuccesseur,
    Nom,
    Prenom,
    Telephone,
    Email,
    Ids,
}

impl AsString for Hdd {
    fn as_str(&self) -> &'static str {
        match self {
            Hdd::Table => "HDD",
            Hdd::Id => "ID",
            Hdd::Pce => "PCE",
            Hdd::IdSource => "Id_source",
            Hdd::RaisonSociale => "Raison_sociale",
            Hdd::Siret => "SIRET",
            Hdd::SiretSuccesseur => "SIRET successeur",
            Hdd::Nom => "Nom",
            Hdd::Prenom => "Prenom",
            Hdd::Telephone => "Telephone",
            Hdd::Email => "Email",
            Hdd::Ids => "IDS",
        }
    }
}
#[derive(Debug, serde::Deserialize, serde::Serialize, sqlx::FromRow)]
pub struct HddSchema {
    #[serde(rename = "Raison_sociale")]
    pub raison_sociale: Option<String>,

    #[serde(rename = "SIRET")]
    pub siret: Option<f64>,

    #[serde(rename = "Nom")]
    pub nom: Option<String>,

    #[serde(rename = "Prenom")]
    pub prenom: Option<String>,

    #[serde(rename = "Telephone")]
    pub telephone: Option<f64>,

    #[serde(rename = "Email")]
    pub email: Option<String>,

    #[serde(rename = "SIRET successeur")]
    pub siret_successeur: Option<f64>,

    #[serde(rename = "Id_source")]
    pub id_source: Option<i32>,

    #[serde(rename = "PCE")]
    pub pce: Option<f64>,

    #[serde(rename = "ID")]
    pub id: i32,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, sqlx::FromRow)]
pub struct HddSchemaCSV {
    #[serde(rename = "Raison_sociale")]
    pub raison_sociale: Option<String>,

    #[serde(rename = "SIRET")]
    pub siret: Option<f64>,

    #[serde(rename = "Nom")]
    pub nom: Option<String>,

    #[serde(rename = "Prenom")]
    pub prenom: Option<String>,

    #[serde(rename = "Telephone")]
    pub telephone: Option<f64>,

    #[serde(rename = "Email")]
    pub email: Option<String>,

    #[serde(rename = "SIRET successeur")]
    pub siret_successeur: Option<f64>,

    #[serde(rename = "Id_source")]
    pub id_source: Option<i32>,

    #[serde(rename = "PCE")]
    pub pce: Option<f64>,

    // #[serde(rename = "ID")]
    // pub id: i32,
}
