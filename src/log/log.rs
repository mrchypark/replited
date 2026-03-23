use std::num::NonZeroUsize;

use logforth::append::file::FileBuilder;
use logforth::layout::TextLayout;
use logforth::record::LevelFilter;
use tracing_subscriber::EnvFilter;

use crate::config::LogConfig;
use crate::config::LogLevel;
use crate::error::Error;
use crate::error::Result;

pub fn init_log(log_config: LogConfig) -> Result<()> {
    let tracing_level = match &log_config.level {
        LogLevel::Error => "error",
        LogLevel::Warn => "warn",
        LogLevel::Info => "info",
        LogLevel::Debug => "debug",
        LogLevel::Trace => "trace",
        LogLevel::Off => "off",
    };
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(tracing_level))
        .with_target(false)
        .with_ansi(false)
        .try_init()
        .map_err(|error| Error::from_string(error.to_string()))?;

    let level: LevelFilter = match &log_config.level {
        LogLevel::Error => LevelFilter::Error,
        LogLevel::Warn => LevelFilter::Warn,
        LogLevel::Info => LevelFilter::Info,
        LogLevel::Debug => LevelFilter::Debug,
        LogLevel::Trace => LevelFilter::Trace,
        LogLevel::Off => LevelFilter::Off,
    };

    let file = FileBuilder::new(log_config.dir, "replited")
        .rollover_size(NonZeroUsize::new(1024 * 4096).unwrap()) // bytes
        .max_log_files(NonZeroUsize::new(9).unwrap())
        .filename_suffix("log")
        .layout(TextLayout::default().no_color())
        .build()
        .map_err(Error::from_std_error)?;

    if let Err(error) = logforth::starter_log::builder()
        .dispatch(|d| d.filter(level).append(file))
        .try_apply()
    {
        let error_text = error.to_string();
        if !error_text.contains("already setup")
            && !error_text.contains("already initialized")
        {
            return Err(Error::from_string(error_text));
        }
    }

    Ok(())
}
