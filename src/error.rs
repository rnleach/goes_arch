use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};

pub struct GoesArchError(String);

impl GoesArchError {
    pub fn new(message: &str) -> Self {
        GoesArchError(message.into())
    }
}

impl Debug for GoesArchError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl Display for GoesArchError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl Error for GoesArchError {}
