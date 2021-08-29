use std::{
    error::Error,
    fs::{create_dir_all, read_dir, File},
    io::Write,
    path::{Path, PathBuf},
};

use crate::{error::GoesArchError, product::Product, remote::RemoteArchive, satellite::Satellite};
use chrono::{
    naive::{NaiveDate, NaiveDateTime},
    Datelike, Duration, Timelike,
};

pub struct Archive<T: RemoteArchive> {
    root: PathBuf,
    remote: T,
}

const HOUR_COMPLET_FNAME: &str = "hour_complete.txt";

impl<RA> Archive<RA>
where
    RA: RemoteArchive,
{
    pub fn connect<P>(root_path: P, remote: RA) -> Self
    where
        P: Into<PathBuf>,
    {
        let root = root_path.into();

        Self { root, remote }
    }

    pub fn retrieve_paths(
        &self,
        sat: Satellite,
        prod: Product,
        start: NaiveDateTime,
        end: NaiveDateTime,
    ) -> Result<Vec<PathBuf>, Box<dyn Error>> {
        assert!(start < end);

        let earliest = match sat {
            Satellite::GOES16 => NaiveDate::from_ymd(2017, 12, 18).and_hms(17, 30, 0),
            Satellite::GOES17 => NaiveDate::from_ymd(2017, 2, 12).and_hms(18, 0, 0),
        };

        let start = if start < earliest { earliest } else { start };

        if end < start {
            return Err(Box::new(GoesArchError::new("Invalid satellite dates.")));
        }

        let too_old_to_not_be_done = chrono::Utc::now().naive_utc() - Duration::days(1);

        let mut to_ret = vec![];

        let mut curr_time = start;

        while curr_time <= end {
            let dir = self.build_path(sat, prod, curr_time);

            if !Self::path_is_complete(&dir, prod)? {
                let remote_filenames = self
                    .remote
                    .retrieve_remote_filenames(sat, prod, curr_time)?;

                for remote_fname in &remote_filenames {
                    let local_path = dir.join(remote_fname);
                    if !local_path.exists() {
                        let data: Vec<u8> =
                            self.remote
                                .retrieve_remote_file(sat, prod, curr_time, remote_fname)?;
                        let mut f = File::create(local_path)?;
                        f.write_all(&data)?;
                    }
                }

                if curr_time < too_old_to_not_be_done {
                    Self::mark_dir_as_complete(&dir)?;
                }
            }

            for entry in read_dir(&dir)? {
                let entry = entry?;
                let pth = entry.path();

                if pth.is_dir() {
                    continue;
                }

                if let Some(ext) = pth.extension().map(|p| p.to_string_lossy()) {
                    if ext != "nc" {
                        continue;
                    }
                }

                to_ret.push(pth);
            }

            curr_time += Duration::hours(1);
        }

        Ok(to_ret)
    }

    fn path_is_complete(pth: &Path, prod: Product) -> Result<bool, Box<dyn Error>> {
        if !pth.exists() {
            create_dir_all(pth)?;
            return Ok(false);
        }

        let completion_marker = pth.join(HOUR_COMPLET_FNAME);

        if completion_marker.exists() {
            return Ok(true);
        }

        let num_files: usize = read_dir(&pth)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter_map(|pth| pth.extension().map(|ext| ext.to_string_lossy() == "nc"))
            .filter(|ext_bool| *ext_bool)
            .count();

        if num_files >= prod.max_num_per_hour() as usize {
            Self::mark_dir_as_complete(pth)?;
            return Ok(true);
        }

        Ok(false)
    }

    fn mark_dir_as_complete(pth: &Path) -> Result<(), Box<dyn Error>> {
        let now = chrono::Utc::now().naive_utc();
        let completion_marker = pth.join(HOUR_COMPLET_FNAME);

        let mut f = File::create(completion_marker)?;
        let complete_time = format!("{}\n", now);

        f.write_all(complete_time.as_bytes())?;

        Ok(())
    }

    fn build_path(
        &self,
        sat: Satellite,
        prod: Product,
        valid_time_to_the_hour: NaiveDateTime,
    ) -> PathBuf {
        let mut pth = PathBuf::new();

        pth.push(&self.root);
        pth.push::<&'static str>(sat.into());
        pth.push::<&'static str>(prod.into());

        let year = valid_time_to_the_hour.year();
        let day = valid_time_to_the_hour.ordinal();
        let hour = valid_time_to_the_hour.hour();
        pth.push(&format!("{:04}/{:03}/{:02}", year, day, hour));

        pth
    }
}
