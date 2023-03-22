use crate::{error::GoesArchError, product::Product, remote::RemoteArchive, satellite::Satellite};
use chrono::{naive::NaiveDateTime, Datelike, Timelike};
use s3::{bucket::Bucket, creds::Credentials, region::Region};
use std::error::Error;

#[derive(Debug, Clone)]
pub struct AmazonS3NoaaBigData {
    bucket_g16: Bucket,
    bucket_g17: Bucket,
    bucket_g18: Bucket,
    num_max_downloads: usize,
}

impl AmazonS3NoaaBigData {
    fn get_storage_location(
        &self,
        sat: Satellite,
        prod: Product,
        valid_hour: NaiveDateTime,
    ) -> (&Bucket, String) {
        let bucket = self.get_bucket(sat);

        let prod: &'static str = prod.into();
        let year = valid_hour.year();
        let day = valid_hour.ordinal();
        let hour = valid_hour.hour();

        (bucket, format!("{}/{}/{:03}/{:02}/", prod, year, day, hour))
    }

    fn get_bucket(&self, sat: Satellite) -> &Bucket {
        match sat {
            Satellite::GOES16 => &self.bucket_g16,
            Satellite::GOES17 => &self.bucket_g17,
            Satellite::GOES18 => &self.bucket_g18,
        }
    }
}

impl RemoteArchive for AmazonS3NoaaBigData {
    fn connect(num_max_downloads: usize) -> Result<Self, Box<dyn Error>>
    where
        Self: Sized,
    {
        let region: Region = "us-east-1".parse()?;
        let credentials = Credentials::anonymous()?;
        let bucket_str_g18 = "noaa-goes18";
        let bucket_str_g17 = "noaa-goes17";
        let bucket_str_g16 = "noaa-goes16";

        let bucket_g16 = {
            let region = region.clone();
            let credentials = credentials.clone();
            Bucket::new(&bucket_str_g16, region, credentials)?
        };

        let bucket_g17 = {
            let region = region.clone();
            let credentials = credentials.clone();
            Bucket::new(&bucket_str_g17, region, credentials)?
        };

        let bucket_g18 = {
            let region = region;
            let credentials = credentials;
            Bucket::new(&bucket_str_g18, region, credentials)?
        };

        Ok(AmazonS3NoaaBigData {
            bucket_g16,
            bucket_g17,
            bucket_g18,
            num_max_downloads,
        })
    }

    fn retrieve_remote_filenames(
        &self,
        sat: Satellite,
        prod: Product,
        valid_hour: NaiveDateTime,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let (bucket, common_prefix) = self.get_storage_location(sat, prod, valid_hour);

        let results = bucket.list_blocking(common_prefix, Some("/".into()))?;

        let mut fnames: Vec<String> = vec![];
        for res in results {
            for obj in &res.contents {
                let path = &obj.key;
                if let Some(i) = path.rfind("/") {
                    let fname = String::from(&path[(i + 1)..]);
                    fnames.push(fname);
                }
            }
        }

        Ok(fnames)
    }

    fn retrieve_remote_file(
        &self,
        sat: Satellite,
        prod: Product,
        valid_hour: NaiveDateTime,
        remote_path: &str,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        let (bucket, common_prefix) = self.get_storage_location(sat, prod, valid_hour);

        let key = common_prefix + remote_path;

        let (data, code) = bucket.get_object_blocking(key)?;

        if code != 200 {
            return Err(Box::new(GoesArchError::new("Download error")));
        }

        Ok(data)
    }

    fn max_downloads(&self) -> usize {
        self.num_max_downloads
    }
}
