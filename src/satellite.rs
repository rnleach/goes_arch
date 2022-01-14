use crate::Product;
use chrono::{NaiveDate, NaiveDateTime};
use strum::IntoStaticStr;

#[derive(Clone, Copy, Debug, IntoStaticStr)]
pub enum Satellite {
    #[strum(serialize = "G16")]
    GOES16,
    #[strum(serialize = "G17")]
    GOES17,
}

impl Satellite {
    pub fn earliest_operational_date(&self, prod: Product) -> NaiveDateTime {
        match (*self, prod) {
            (_, Product::FDCM) => NaiveDate::from_ymd(2021, 5, 17).and_hms(12, 0, 0),
            (Satellite::GOES16, _) => NaiveDate::from_ymd(2017, 12, 18).and_hms(12, 0, 0),
            (Satellite::GOES17, _) => NaiveDate::from_ymd(2019, 2, 12).and_hms(12, 0, 0),
        }
    }
}
