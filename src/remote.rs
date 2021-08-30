use std::error::Error;

use crate::{product::Product, satellite::Satellite};
use chrono::naive::NaiveDateTime;

pub trait RemoteArchive: Clone + Send {
    fn connect() -> Result<Self, Box<dyn Error>>
    where
        Self: Sized;

    fn retrieve_remote_filenames(
        &self,
        sat: Satellite,
        prod: Product,
        valid_hour: NaiveDateTime,
    ) -> Result<Vec<String>, Box<dyn Error>>;

    fn retrieve_remote_file(
        &self,
        sat: Satellite,
        prod: Product,
        valid_hour: NaiveDateTime,
        remote_path: &str,
    ) -> Result<Vec<u8>, Box<dyn Error>>;
}
