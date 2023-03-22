use crate::Product;
use chrono::{NaiveDate, NaiveDateTime};
use strum::IntoStaticStr;

#[derive(Clone, Copy, Debug, IntoStaticStr)]
pub enum Satellite {
    #[strum(serialize = "G16")]
    GOES16,
    #[strum(serialize = "G17")]
    GOES17,
    #[strum(serialize = "G18")]
    GOES18,
}

impl Satellite {
    pub fn earliest_operational_date(&self, prod: Product) -> NaiveDateTime {
        match (*self, prod) {
            (Satellite::GOES16 | Satellite::GOES17, Product::FDCM) => {
                NaiveDate::from_ymd_opt(2021, 5, 17)
                    .and_then(|d| d.and_hms_opt(12, 0, 0))
                    .unwrap()
            }
            (Satellite::GOES16, _) => NaiveDate::from_ymd_opt(2017, 12, 18)
                .and_then(|d| d.and_hms_opt(12, 0, 0))
                .unwrap(),
            (Satellite::GOES17, _) => NaiveDate::from_ymd_opt(2019, 2, 12)
                .and_then(|d| d.and_hms_opt(12, 0, 0))
                .unwrap(),
            (Satellite::GOES18, _) => NaiveDate::from_ymd_opt(2023, 1, 17)
                .and_then(|d| d.and_hms_opt(12, 0, 0))
                .unwrap(),
        }
    }
}
