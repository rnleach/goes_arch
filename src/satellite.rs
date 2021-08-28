use strum::IntoStaticStr;

#[derive(Clone, Copy, Debug, IntoStaticStr)]
pub enum Satellite {
    #[strum(serialize = "G16")]
    GOES16,
    #[strum(serialize = "G17")]
    GOES17,
}
