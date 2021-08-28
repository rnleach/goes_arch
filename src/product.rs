use strum::IntoStaticStr;

#[derive(Clone, Copy, Debug, IntoStaticStr)]
pub enum Product {
    #[strum(serialize = "ABI-L2-FDCC")]
    FDCC,
    #[strum(serialize = "ABI-L2-FDCM")]
    FDCM,
    #[strum(serialize = "ABI-L2-FDCF")]
    FDCF,
}

impl Product {
    pub fn max_num_per_hour(&self) -> i32 {
        match *self {
            Product::FDCM => 60,
            Product::FDCC => 12,
            Product::FDCF => 6,
        }
    }
}
