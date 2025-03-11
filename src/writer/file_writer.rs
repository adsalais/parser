///
/// Write data to a file
///
pub struct FileWriter {
    writer: BufWriter<File>,
}
impl FileWriter {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        const FILE_BUFFER_OUTPUT_CAPACITY: usize = 1024 * 1024;

        let outputfile = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let writer = BufWriter::with_capacity(FILE_BUFFER_OUTPUT_CAPACITY, outputfile);
        Ok(Self { writer })
    }
}
impl OutputWriter for FileWriter {
    fn write(&mut self, data: Tuple) -> Result<(), Error> {
        let line = data.to_json_string()?;
        self.writer.write_all(line.as_bytes())?;
        self.writer.write_all("\n".as_bytes())?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
use std::{cell::RefCell, rc::Rc};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use crate::{
    Error,
    output::{OutputWriter, Tuple},
};
///
/// Write a few lines in a vector
///
#[cfg(test)]
pub struct MemoryWriter {
    buffer: Rc<RefCell<Vec<String>>>,
    max_row: usize,
}
#[cfg(test)]
impl MemoryWriter {
    pub fn new(max_row: usize) -> Self {
        let buffer = Rc::new(RefCell::new(Vec::with_capacity(max_row)));
        Self { buffer, max_row }
    }
    pub fn get_buffer(&self) -> Rc<RefCell<Vec<String>>> {
        self.buffer.clone()
    }
}
#[cfg(test)]
impl OutputWriter for MemoryWriter {
    fn write(&mut self, value: Tuple) -> Result<(), Error> {
        let mut buff = self.buffer.borrow_mut();
        if buff.len() < self.max_row {
            let line = value.to_json_string()?;
            buff.push(line);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
