use std::collections::HashMap;
use lazy_static::lazy_static;

pub const SPECIAL_CIVILITIES: [&str; 9] = [
    "DOCTEUR",
    "GÉNÉRAL",
    "COMPTE",
    "INGÉNIEUR GÉNÉRAL",
    "PRÉFET",
    "PROFESSEUR",
    "MONSEIGNEUR",
    "SŒUR",
    "COMMISSAIRE",
];

lazy_static! {
    pub static ref CIVILITE_MAP: HashMap<&'static str, &'static str> = {
        let mut map = HashMap::new();
        map.insert("MONSIEUR", "MONSIEUR");
        map.insert("M", "MONSIEUR");
        map.insert("M.", "MONSIEUR");
        map.insert("MR", "MONSIEUR");
        map.insert("MM", "MONSIEUR");
        map.insert("M(ESPACE)", "MONSIEUR");
        map.insert("MADAME", "MADAME");
        map.insert("MME", "MADAME");
        map.insert("MRS", "MADAME");
        map.insert("MS", "MADAME");
        map.insert("MLLE", "MADAME");
        map.insert("MAD", "MADAME");
        map.insert("MADEMOISELLE", "MADAME");
        map
    };
}

pub enum Transform {
    Nom,
    Prenom,
    Civilite,
    Email,
    RaisonSociale,
    Telephone,
    // Add other variants as needed
}
