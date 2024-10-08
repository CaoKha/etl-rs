pub mod hdd;
pub mod jdd;

pub enum SchemasEnum {
    Jdd,
    Hdd,
}

pub trait AsString {
    fn as_str(&self) -> &'static str;
}
