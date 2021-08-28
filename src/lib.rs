/**************************************************************************************************
 *                                           Public API
 *************************************************************************************************/
pub use crate::{
    archive::Archive, error::GoesArchError, product::Product, remote::RemoteArchive,
    s3_remote::AmazonS3NoaaBigData, satellite::Satellite,
};
/**************************************************************************************************
 *                                      Private Implementation
 *************************************************************************************************/
mod archive;
mod error;
mod product;
mod remote;
mod s3_remote;
mod satellite;
