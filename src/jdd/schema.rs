use sea_query::Iden;

#[derive(Iden)]
pub enum Jdd {
    Table,
    Id,
    RaisonSociale,
    Siret,
    Siren,
    Ape,
    CodeNaf,
    LibeleNaf,
    Civilite,
    Nom,
    Prenom,
    Telephone,
    Email,
    Address,
    CodePostale,
    Region,
    Pays,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, sqlx::FromRow)]
pub struct JddSchema {
    #[serde(rename = "RAISON_SOCIALE")]
    pub raison_sociale: Option<String>,

    #[serde(rename = "SIRET")]
    pub siret: Option<String>,

    #[serde(rename = "SIREN")]
    pub siren: Option<String>,

    #[serde(rename = "APE")]
    pub ape: Option<String>,

    #[serde(rename = "CODE_NAF")]
    pub code_naf: Option<String>,

    #[serde(rename = "LIBELE_NAF")]
    pub libele_naf: Option<String>,

    #[serde(rename = "CIVILITE")]
    pub civilite: Option<String>,

    #[serde(rename = "NOM")]
    pub nom: Option<String>,

    #[serde(rename = "PRENOM")]
    pub prenom: Option<String>,

    #[serde(rename = "TELEPHONE")]
    pub telephone: Option<String>,

    #[serde(rename = "email")]
    pub email: Option<String>,

    #[serde(rename = "address")]
    pub address: Option<String>,

    #[serde(rename = "CODE POSTALE")]
    pub code_postale: Option<String>,

    #[serde(rename = "REGION")]
    pub region: Option<String>,

    #[serde(rename = "PAYS")]
    pub pays: Option<String>,
}
