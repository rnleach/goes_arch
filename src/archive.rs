use std::{
    error::Error,
    fs::{create_dir_all, read_dir, File},
    io::Write,
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
};

use crate::{error::GoesArchError, product::Product, remote::RemoteArchive, satellite::Satellite};
use chrono::{naive::NaiveDateTime, Datelike, Duration, Timelike};
use crossbeam_channel::{bounded, Receiver, Sender};

pub struct Archive<T: RemoteArchive> {
    root: PathBuf,
    remote: T,
}

impl<RA: 'static> Archive<RA>
where
    RA: RemoteArchive + Clone + Send,
{
    pub fn connect<P>(root_path: P, remote: RA) -> Self
    where
        P: Into<PathBuf>,
    {
        let root = root_path.into();
        log::info!("Connected to archive at: {:?}", &root);
        Self { root, remote }
    }

    pub fn retrieve_paths(
        &self,
        sat: Satellite,
        prod: Product,
        start: NaiveDateTime,
        end: NaiveDateTime,
    ) -> Result<Vec<PathBuf>, Box<dyn Error>> {
        let (start, end) = Self::validate_dates(sat, start, end)?;

        let (to_path_accumulator, paths_to_accumulate) = bounded(100);
        let (to_downloader, needs_downloaded) = bounded(100);
        let (to_saver, from_downloader) = bounded(10);

        let accum_thrd = Self::start_accumulator_thread(paths_to_accumulate)?;
        self.start_download_thread(
            sat,
            prod,
            needs_downloaded,
            to_saver,
            to_path_accumulator.clone(),
        )?;
        let save_thrd = Self::start_save_thread(from_downloader, to_path_accumulator.clone())?;

        for curr_time in (0..)
            .map(|i| end - Duration::hours(i))
            .take_while(|time| *time >= start)
        {
            let dir = self.build_path(sat, prod, curr_time);

            if Self::path_is_complete(&dir, prod)? {
                to_path_accumulator.send(dir)?;
            } else {
                to_downloader.send((dir, curr_time))?;
            }
        }

        drop(to_downloader);
        drop(to_path_accumulator);
        save_thrd.join().unwrap();
        let to_ret = accum_thrd.join().unwrap();

        Ok(to_ret)
    }
}

// Private methods and associated functions.

const HOUR_COMPLETE_FNAME: &str = "hour_complete.txt";

impl<RA: 'static> Archive<RA>
where
    RA: RemoteArchive + Clone + Send,
{
    fn start_save_thread(
        file_paths: Receiver<(PathBuf, Vec<u8>)>,
        to_accumulator: Sender<PathBuf>,
    ) -> Result<JoinHandle<()>, Box<dyn Error>> {
        let jh = thread::Builder::new()
            .name("Save Thread".into())
            .spawn(move || {
                for (pth, data) in file_paths {
                    let mut f = match File::create(&pth) {
                        Ok(f) => f,
                        Err(err) => {
                            log::error!("Error creating file: {:?} : {}", pth, err);
                            continue;
                        }
                    };

                    match f.write_all(&data) {
                        Ok(()) => {}
                        Err(err) => {
                            log::error!("Error writing data to disk: {:?} : {}", pth, err);
                        }
                    };

                    log::debug!("Saved {:?}", pth);
                    to_accumulator.send(pth).unwrap();
                }
            })?;

        Ok(jh)
    }

    fn start_download_thread(
        &self,
        sat: Satellite,
        prod: Product,
        local_dirs: Receiver<(PathBuf, NaiveDateTime)>,
        to_data_saver: Sender<(PathBuf, Vec<u8>)>,
        to_accumulator: Sender<PathBuf>,
    ) -> Result<(), Box<dyn Error>> {
        const NUM_DOWNLOADERS: usize = 3;

        let pool = threadpool::ThreadPool::with_name("Download Thread".to_owned(), NUM_DOWNLOADERS);

        for _ in 0..NUM_DOWNLOADERS {
            let remote = self.remote.clone();
            let to_data_saver = to_data_saver.clone();
            let to_accumulator = to_accumulator.clone();
            let local_dirs = local_dirs.clone();

            pool.execute(move || {
                for (dir, curr_time) in local_dirs {
                    log::info!("Downloading data for directory: {:?}", &dir);

                    let remote_filenames =
                        match remote.retrieve_remote_filenames(sat, prod, curr_time) {
                            Ok(fnames) => fnames,
                            Err(err) => {
                                log::error!("Error retreiving remote file names: {}", err);
                                continue;
                            }
                        };

                    for remote_fname in &remote_filenames {
                        let local_path = dir.join(remote_fname);
                        if local_path.exists() {
                            log::debug!("Skipping download for {:?}", local_path);
                            to_accumulator.send(local_path).unwrap();
                        } else {
                            let data: Vec<u8> = match remote.retrieve_remote_file(
                                sat,
                                prod,
                                curr_time,
                                remote_fname,
                            ) {
                                Ok(data) => data,
                                Err(err) => {
                                    log::error!(
                                        "Error downloading data: {} : {}",
                                        remote_fname,
                                        err
                                    );
                                    continue;
                                }
                            };

                            to_data_saver.send((local_path, data)).unwrap();
                        }
                    }

                    let now = chrono::Utc::now().naive_utc();
                    let completion_marker = dir.join(HOUR_COMPLETE_FNAME);
                    let complete_time = format!("{}\n", now).as_bytes().to_vec();
                    to_data_saver.send((completion_marker, complete_time)).unwrap();
                }
            });
        }

        Ok(())
    }

    fn start_accumulator_thread(
        paths: Receiver<PathBuf>,
    ) -> Result<JoinHandle<Vec<PathBuf>>, Box<dyn Error>> {
        let th = thread::Builder::new()
            .name("PathBuf Accumulator".to_owned())
            .spawn(|| {
                let mut to_ret = vec![];

                for pth in paths {
                    if pth.is_dir() {
                        let read_dir = match read_dir(&pth) {
                            Ok(read_dir) => read_dir,
                            Err(err) => {
                                log::error!("Error reading directory: {:?} : {}", pth, err);
                                continue;
                            }
                        };

                        for entry_res in read_dir {
                            let entry = match entry_res {
                                Ok(entry) => entry,
                                Err(err) => {
                                    log::error!("Error reading directory entry: {}", err);
                                    continue;
                                }
                            };

                            let file_pth = entry.path();

                            if file_pth.is_dir() {
                                continue;
                            }

                            if let Some(ext) = file_pth.extension().map(|p| p.to_string_lossy()) {
                                if ext != "nc" {
                                    continue;
                                }
                            }

                            to_ret.push(file_pth);
                        }
                    } else {
                        to_ret.push(pth);
                    }
                }

                to_ret
            })?;

        Ok(th)
    }

    fn validate_dates(
        sat: Satellite,
        start: NaiveDateTime,
        end: NaiveDateTime,
    ) -> Result<(NaiveDateTime, NaiveDateTime), GoesArchError> {
        log::info!("start - {} end {}", start, end);

        if end < start {
            log::error!("End before start: start - {} end - {}", start, end);
            return Err(GoesArchError::new("Invalid satellite dates."));
        }

        let earliest = sat.earliest_operational_date();
        let valid_start = if start < earliest { earliest } else { start };

        if valid_start != start {
            log::warn!("valid start time was adjusted to start - {}", valid_start);
        }

        if end < valid_start {
            log::error!("End before start: start - {} end - {}", valid_start, end);
            Err(GoesArchError::new("Invalid satellite dates."))
        } else {
            Ok((valid_start, end))
        }
    }

    fn path_is_complete(pth: &Path, prod: Product) -> Result<bool, Box<dyn Error>> {
        if !pth.exists() {
            create_dir_all(pth)?;
            log::debug!("Creating path: {:?}", pth);
            return Ok(false);
        }

        let completion_marker = pth.join(HOUR_COMPLETE_FNAME);

        if completion_marker.exists() {
            log::debug!("Completion marker found path: {:?}", pth);
            return Ok(true);
        }

        let num_files: usize = read_dir(&pth)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter_map(|pth| pth.extension().map(|ext| ext.to_string_lossy() == "nc"))
            .filter(|ext_bool| *ext_bool)
            .count();

        if num_files >= prod.max_num_per_hour() as usize {
            log::debug!(
                "Enough files found in path to mark it as complete: {:?}",
                pth
            );
            Self::mark_dir_as_complete(pth)?;
            return Ok(true);
        }

        log::debug!("Cannot confirm this path is complete: {:?}", pth);
        Ok(false)
    }

    fn mark_dir_as_complete(pth: &Path) -> Result<(), Box<dyn Error>> {
        let now = chrono::Utc::now().naive_utc();
        let completion_marker = pth.join(HOUR_COMPLETE_FNAME);

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
