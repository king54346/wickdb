
#![allow(clippy::rc_buffer)]
#[macro_use]
extern crate log;
extern crate crc32fast;
extern crate crossbeam_channel;
extern crate crossbeam_utils;
extern crate slog;
extern crate slog_async;
extern crate slog_term;
#[macro_use]
extern crate num_derive;
extern crate bytes;
extern crate quick_error;
extern crate rand;
extern crate snap;

pub mod batch;
pub mod cache;
mod util;
#[macro_use]
mod error;
mod compaction;
pub mod db;
pub mod filter;
mod iterator;
mod logger;
pub mod mem;
pub mod options;
mod record;
mod snapshot;
mod sstable;
pub mod storage;
mod table_cache;
mod version;

pub use batch::WriteBatch;
pub use cache::Cache;
pub use compaction::ManualCompaction;
pub use db::{WickDB, DB};
pub use error::{Error, Result};
pub use filter::bloom::BloomFilter;
pub use iterator::Iterator;
pub use log::{LevelFilter, Log};
pub use options::{CompressionType, Options, ReadOptions, WriteOptions};
pub use sstable::block::Block;
pub use storage::*;
pub use util::comparator::{BytewiseComparator, Comparator};
pub use util::varint::*;
