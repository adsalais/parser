use core::time;
use std::{
    fs,
    path::PathBuf,
    process::Command,
    thread,
    time::{Duration, Instant},
};

use flume::Sender;
use log::{error, info, warn};

use crate::{
    Error,
    configuration::{Configuration, ParserConfig, ParserType},
    input::{csv::parse_csv, evtx::parse_evtx, hive::parse_hive, srum::SrumParser},
    output::{Fields, OutputConfig},
};

///
/// Message consumed by the Archive management threads
///
#[derive(Debug)]
pub struct ArchiveMsg {
    folder: PathBuf,
    in_error: bool,
    is_temp_folder: bool,
    decompress_duration: Duration,
}

///
/// Message consumed by the parsing  threads
///
pub struct ParseMsg {
    file: PathBuf,
    config: ParserType,
    fields: Fields,
    reply: Sender<Result<FileResultMsg, Error>>,
}

///
/// Message sent after a successful file parsing
///
pub struct FileResultMsg {
    pub file: String,
    pub num_rows: usize,
    pub duration: Duration,
}

///
/// Message sent after a successful archive parsing
///
pub struct ArchiveResultMsg {
    pub folder: String,
    pub num_errors: usize,
    pub decompress_duration: Duration,
    pub parsing_duration: Duration,
}

///
/// Read every files and directories in the configuration.input folder and process them
///
/// Workflow for archives:                  
///     input.7z --> decrompression service --> archive management service --> file parsing service
///
/// Worflow for folders:
///     folder   --> archive management service --> file parsing service
///
pub fn parse(configuration: Configuration) -> Result<(), Error> {
    //remove temp folder and recreate it
    let _ = fs::remove_dir_all(&configuration.temp_folder);
    fs::create_dir_all(&configuration.temp_folder)?;
    let (reply, receiver) = flume::unbounded::<ArchiveResultMsg>();

    let file_parsing_service = create_parsing_threads(
        thread_number_or_default(configuration.parsing_threads),
        &configuration.output,
        &configuration.client_context,
    );
    let archive_service = create_archive_threads(
        thread_number_or_default(configuration.archive_threads),
        &configuration.parsers,
        file_parsing_service,
        reply,
    );
    let decompression_service = create_decompression_threads(
        thread_number_or_default(configuration.decompression_threads),
        &configuration.temp_folder.into(),
        archive_service,
    );

    let read_dir = fs::read_dir(&configuration.input_folder)?;

    for path in read_dir {
        let path = path?;

        if configuration.input_is_decompressed {
            if path.path().is_dir() {
                decompression_service
                    .send(path.path())
                    .map_err(|e| Error::Generic(format!("Error while sending path: {e}")))?;
            } else {
                info!(
                    "expecting decompressed folder, skipping file {}",
                    path.path().display()
                );
            }
        } else if path.path().is_dir() {
            info!(
                "expecting compressed file, skipping folder {}",
                path.path().display()
            );
        } else {
            decompression_service
                .send(path.path())
                .map_err(|e| Error::Generic(format!("Error while sending path: {e}")))?;
        }
    }

    //drop decompression_service to allow receiver to stop when everything is processed
    drop(decompression_service);

    let ten_millis = time::Duration::from_millis(100);

    thread::sleep(ten_millis);

    //wait for every archives to be processed
    while let Ok(result) = receiver.recv() {
        if result.num_errors > 0 {
            warn!(
                "Archive:'{}' processed  with {} error(s).  Archive decompression:{:.2?}. Parsing and writing data:{:.2?}. Errors can be found in the log file",
                result.folder,
                result.num_errors,
                result.decompress_duration,
                result.parsing_duration,
            )
        } else {
            info!(
                "Archive:'{}' processed successfully. Archive decompression:{:.2?}. Parsing and writing data:{:.2?}.",
                result.folder, result.decompress_duration, result.parsing_duration,
            )
        }
    }

    Ok(())
}

///
/// Decompress a 7z archive in a temp folder and send it to the archive management thread
/// Requires the 7z command to be available on the system
///
fn create_decompression_threads(
    num_thread: usize,
    temp_folder: &PathBuf,
    archive_sender: Sender<ArchiveMsg>,
) -> Sender<PathBuf> {
    let (sender, receiver) = flume::unbounded::<PathBuf>();

    for _ in 0..num_thread {
        let receiver = receiver.clone();
        let archive_sender = archive_sender.clone();
        let temp_folder = temp_folder.clone();
        thread::spawn(move || {
            while let Ok(input_path) = receiver.recv() {
                //if a folder, send it directly to rhe the archive thread
                if input_path.is_dir() {
                    let archive_msg = ArchiveMsg {
                        folder: input_path,
                        is_temp_folder: false,
                        in_error: false,
                        decompress_duration: Duration::from_secs(0),
                    };
                    if let Err(e) = archive_sender.send(archive_msg) {
                        error!("an error occured when sending decompressed archive: {e}");
                    }
                    continue;
                }

                let file = input_path.as_path().file_stem().unwrap();
                let mut output_path = temp_folder.clone();
                output_path.push(file);

                let instant = Instant::now();
                let command_output = Command::new("7z")
                    .arg("x")
                    .arg(input_path.to_str().unwrap())
                    .arg(format!("-o{}", temp_folder.to_str().unwrap()))
                    .arg("-y")
                    .output()
                    .unwrap();

                let mut archive_msg = ArchiveMsg {
                    folder: output_path,
                    is_temp_folder: true,
                    in_error: false,
                    decompress_duration: instant.elapsed(),
                };

                if !command_output.status.success() {
                    archive_msg.in_error = true;
                    let res = String::from_utf8_lossy(&command_output.stderr).to_string();
                    error!("{}", res.replace("\n", " "));
                }

                if let Err(e) = archive_sender.send(archive_msg) {
                    error!("an error occured when sending decompressed archive: {e}");
                }
            }
        });
    }
    sender
}

///
/// Archive management threads:
/// - read the content of the provideed folder and send the files to the parsing thread
/// - upon completion delete the temporary folder if needed
///
fn create_archive_threads(
    num_thread: usize,
    parsers: &[ParserConfig],
    file_parsing_sender: Sender<ParseMsg>,
    archive_reply: Sender<ArchiveResultMsg>,
) -> Sender<ArchiveMsg> {
    //bounded to limit the number of temporary decompressed folders
    let (sender, receiver) = flume::bounded::<ArchiveMsg>(1);

    for _ in 0..num_thread {
        let archive_receiver = receiver.clone();
        let file_parsing_sender = file_parsing_sender.clone();
        let archive_reply = archive_reply.clone();
        let parsers = parsers.to_owned();
        thread::spawn(move || {
            while let Ok(archive_msg) = archive_receiver.recv() {
                let instant = Instant::now();
                let archive_name = archive_msg
                    .folder
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();

                //if decompression is in error return the result directly
                if archive_msg.in_error {
                    let result = ArchiveResultMsg {
                        folder: archive_name,
                        num_errors: 1,
                        decompress_duration: archive_msg.decompress_duration,
                        parsing_duration: instant.elapsed(),
                    };
                    if let Err(e) = archive_reply.send(result) {
                        error!("an error occured when sending archive result: {e}");
                    }
                    continue;
                }

                let mut num_errors = 0;
                let (reply, receiver) = flume::unbounded::<Result<FileResultMsg, Error>>();

                //iterate the folder to find file that match the file filters
                let read_dir = fs::read_dir(&archive_msg.folder).unwrap();
                for path in read_dir {
                    let path = path.unwrap();
                    let file_name = path.file_name().to_string_lossy().to_string();

                    let mut found = false;
                    for parser in &parsers {
                        if parser.file_filter.is_match(&file_name) {
                            if found {
                                num_errors += 1;
                                error!(
                                    "Archive:'{archive_name}', file:'{file_name}' has already been matched by a filter",
                                )
                            } else {
                                found = true;
                                let fields = Fields::new(
                                    &archive_name,
                                    &file_name,
                                    &archive_name,
                                    &file_name,
                                );

                                let msg = ParseMsg {
                                    file: path.path(),
                                    config: parser.parser.clone(),
                                    fields,
                                    reply: reply.clone(),
                                };

                                if let Err(e) = file_parsing_sender.send(msg) {
                                    error!(
                                        "an error occured when sending file parsing message: {e}"
                                    );
                                }
                            }
                        }
                    }

                    if !found {
                        info!(
                            "Archive:'{archive_name}' file:'{file_name}' did not match any pattern",
                        )
                    }
                }
                //if not dropped, the receiver will never stop
                drop(reply);

                //wait for every files to be processed
                while let Ok(result) = receiver.recv() {
                    match result {
                        Ok(msg) => info!(
                            "Archive:'{archive_name}' file:'{}'. {} rows processed in {:.2?}",
                            msg.file, msg.num_rows, msg.duration
                        ),
                        Err(e) => {
                            num_errors += 1;
                            error!("Error while processing archive {archive_name}: {e}")
                        }
                    }
                }
                let result = ArchiveResultMsg {
                    folder: archive_msg
                        .folder
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string(),
                    num_errors,
                    decompress_duration: archive_msg.decompress_duration,
                    parsing_duration: instant.elapsed(),
                };

                //removes the temporary folder
                if archive_msg.is_temp_folder {
                    let _ = fs::remove_dir_all(archive_msg.folder);
                }
                if let Err(e) = archive_reply.send(result) {
                    error!("an error occured when sending archive result: {e}");
                }
            }
        });
    }

    sender
}

///
/// threads that parse the file using the right parser.   
///
pub fn create_parsing_threads(
    num_thread: usize,
    output_configs: &[OutputConfig],
    client_context: &str,
) -> Sender<ParseMsg> {
    let (sender, receiver) = flume::unbounded::<ParseMsg>();
    let num_thread = thread_number_or_default(num_thread);
    for _ in 0..num_thread {
        let receiver = receiver.clone();
        let output_configs = output_configs.to_vec();
        let client_context = client_context.to_string();

        thread::spawn(move || {
            while let Ok(parse_msg) = receiver.recv() {
                let result = parse_file(&parse_msg, &output_configs, &client_context);

                if let Err(e) = parse_msg.reply.send(result) {
                    error!("an error occured when sending parsing result: {e}");
                }
            }
        });
    }
    sender
}

///
/// Parse a file and write result to the output
/// upon success, returns the execution time
///
fn parse_file(
    parse_msg: &ParseMsg,
    output_config: &[OutputConfig],
    client_context: &str,
) -> Result<FileResultMsg, Error> {
    let instant = Instant::now();
    let num_rows = match &parse_msg.config {
        ParserType::csv {
            mapping_file,
            best_effort,
            skip_lines,
        } => {
            let best_effort = best_effort.unwrap_or(false);
            let skip_lines = skip_lines.unwrap_or(0);
            parse_csv(
                &parse_msg.file,
                client_context,
                &parse_msg.fields,
                output_config,
                &mapping_file,
                best_effort,
                skip_lines,
            )?
        }
        ParserType::evtx => parse_evtx(
            &parse_msg.file,
            client_context,
            &parse_msg.fields,
            output_config,
        )?,
        ParserType::hive { root_name } => parse_hive(
            &parse_msg.file,
            client_context,
            &parse_msg.fields,
            root_name,
            output_config,
        )?,
        ParserType::srum => {
            let parser = SrumParser::new(&parse_msg.file)?;
            parser.parse_all_tables(client_context, &parse_msg.fields, output_config)?
        }
    };
    Ok(FileResultMsg {
        file: parse_msg.fields.archive_file.to_owned(),
        num_rows,
        duration: instant.elapsed(),
    })
}

///
/// if thread number is zero, default it to half the number of available cpu threads (minimum 1)
///
fn thread_number_or_default(thread_number: usize) -> usize {
    if thread_number == 0 {
        let half_availaible_threads = thread::available_parallelism().unwrap().get() / 2;
        if half_availaible_threads == 0 {
            1
        } else {
            half_availaible_threads
        }
    } else {
        thread_number
    }
}

#[cfg(test)]
mod tests {
    use regex::Regex;

    use crate::init_log;

    use super::*;

    const TEMP_FOLDER: &str = "data/temp";

    #[test]
    fn decompress_service_error() {
        fs::create_dir_all(TEMP_FOLDER).unwrap();

        let (sender, receiver) = flume::unbounded::<ArchiveMsg>();
        let mut temp_folder: PathBuf = TEMP_FOLDER.into();
        temp_folder.push("decompress_service_error");

        let decompress = create_decompression_threads(2, &temp_folder.clone(), sender);

        // test invalid file
        decompress.send("invalid.7z".into()).unwrap();

        temp_folder.push("invalid");

        match receiver.recv() {
            Ok(res) => {
                assert!(res.in_error);
                assert!(!temp_folder.as_path().exists());
            }

            Err(e) => {
                panic!("{e}");
            }
        }
    }

    #[test]
    fn decompress_service() {
        fs::create_dir_all(TEMP_FOLDER).unwrap();

        let (sender, receiver) = flume::unbounded::<ArchiveMsg>();

        let mut temp_folder: PathBuf = TEMP_FOLDER.into();
        temp_folder.push("decompress_service");

        let decompress = create_decompression_threads(2, &temp_folder.clone(), sender);
        //test valid file
        decompress
            .send("data/archive/compressed/machine1_2025.7z".into())
            .unwrap();

        temp_folder.push("machine1_2025");

        match receiver.recv() {
            Ok(res) => {
                assert!(!res.in_error);
                assert!(temp_folder.as_path().exists());
                let _ = fs::remove_dir_all(temp_folder);
            }

            Err(e) => {
                panic!("{e}");
            }
        }
    }

    #[test]
    fn parsefile() {
        init_log();
        let (reply, _) = flume::unbounded::<Result<FileResultMsg, Error>>();

        let fields = Fields::new("machine", "SRUMDB.dat", "SRUMDB", "SRUDB.dat");
        let parse_msg = ParseMsg {
            file: "data/parser/SRUDB.dat".into(),
            config: ParserType::srum,
            fields,
            reply: reply,
        };

        let temp = "data/test/temp/parsefile";
        let _ = fs::remove_dir_all(temp);
        fs::create_dir_all(temp).unwrap();
        let conf = OutputConfig::file {
            folder: temp.to_string(),
        };
        parse_file(&parse_msg, &vec![conf], "client_name").unwrap();

        let paths = fs::read_dir(format!("{temp}/SRUMDB")).unwrap();
        let mut len = 0;
        let mut count = 0;
        for path in paths {
            let path = path.unwrap();
            len += fs::metadata(path.path()).unwrap().len();
            count += 1;
        }
        assert_eq!(10, count);
        assert_eq!(3038209, len);
        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn parse_compressed_archive() {
        init_log();

        let file_filter = Regex::new("SRUDB.*\\.dat$").unwrap();
        let parser_config = ParserConfig {
            file_filter,
            parser: ParserType::srum,
        };

        let output = "data/temp/parse_compressed";
        let _ = fs::remove_dir_all(output);
        fs::create_dir_all(output).unwrap();
        let output_config = OutputConfig::file {
            folder: output.to_string(),
        };

        let configuration = Configuration {
            client_context: "test_context".to_string(),
            input_folder: "data/archive/compressed".to_string(),
            input_is_decompressed: false,
            temp_folder: "data/temp/".to_string(),
            archive_threads: 0,
            parsing_threads: 0,
            decompression_threads: 0,
            parsers: vec![parser_config],
            output: vec![output_config],
        };

        parse(configuration).unwrap();

        let paths = fs::read_dir(format!("{output}/machine1_2025")).unwrap();
        let mut len = 0;
        let mut count = 0;
        for path in paths {
            let path = path.unwrap();
            len += fs::metadata(path.path()).unwrap().len();
            count += 1;
        }
        let _ = fs::remove_dir_all(output);
        assert_eq!(10, count);
        assert_eq!(3066469, len);
    }

    #[test]
    fn parse_decompressed_archive() {
        init_log();

        let file_filter = Regex::new("SRUDB.*\\.dat$").unwrap();
        let parser_config = ParserConfig {
            file_filter,
            parser: ParserType::srum,
        };

        let output = "data/temp/parse_decompressed";
        let _ = fs::remove_dir_all(output);
        fs::create_dir_all(output).unwrap();
        let output_config = OutputConfig::file {
            folder: output.to_string(),
        };

        let configuration = Configuration {
            client_context: "test_context".to_string(),
            input_folder: "data/archive/decompressed".to_string(),
            input_is_decompressed: true,
            temp_folder: "data/temp/".to_string(),
            archive_threads: 0,
            parsing_threads: 0,
            decompression_threads: 0,
            parsers: vec![parser_config],
            output: vec![output_config],
        };

        parse(configuration).unwrap();

        let paths = fs::read_dir(format!("{output}/machine1_2025")).unwrap();
        let mut len = 0;
        let mut count = 0;
        for path in paths {
            let path = path.unwrap();
            len += fs::metadata(path.path()).unwrap().len();
            count += 1;
        }
        // let _ = fs::remove_dir_all(output);
        assert_eq!(10, count);
        assert_eq!(3066469, len);
    }
}
