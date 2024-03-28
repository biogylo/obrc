use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Read};
use std::ops::Deref;
use std::path::PathBuf;

struct MmapReader {
    map: memmap2::Mmap,
    file: File,
    offset: usize
}

impl Read for MmapReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_left = self.map.as_ref().len()-self.offset;
        let bytes_read = std::cmp::min(bytes_left, buf.len());
        let map_slice = &self.map.deref()[self.offset..self.offset+bytes_read];
        let mut buf_slice = &mut buf[..bytes_read];
        buf_slice.copy_from_slice(map_slice);
        self.offset += bytes_read;
        Ok(bytes_read)
    }
}
impl MmapReader {
    fn bufreader(self) -> BufReader<MmapReader> {
        BufReader::new(self)
    }
    fn new(file: File) -> MmapReader {
        let map = unsafe {
            // Safety: this is a read only map
            memmap2::Mmap::map(&file).expect("Unable to mmap file!")
        };
        let offset = 0;
        MmapReader{
            map,
            file,
            offset
        }
    }
}

fn take_tokens(reader: &mut impl BufRead) -> Option<(Box<[u8]>, isize)>{
    let mut name_buf = vec![];
    let read = reader.read_until(b';',&mut name_buf).unwrap();
    if read <= 1 {
        return None; // EOF reached
    }
    let the_name = Box::from( &name_buf[..read-1]);
    let mut meas_buf = vec![];
    let read = reader.read_until(b'\n',&mut meas_buf).unwrap();
    let end_num = read-2;
    let decimal = meas_buf[end_num]-48;
    let end_rest = read-3;
    let num_buff = meas_buf.get(0..end_rest)
        .unwrap_or_else(|| panic!("Unable to index buffer -> {meas_buf:?} with indices 0 to {end_rest:?}"));
    let is_negative = num_buff[0] == b'-';
    let rest = if is_negative {
        core::str::from_utf8(&num_buff[1..]).unwrap().parse::<u8>()
    } else {
        core::str::from_utf8(&num_buff[..]).unwrap().parse::<u8>()
    }.unwrap_or_else(|_| panic!("Unable to parse i8 from {num_buff:?}"));
    let rest =  ((rest as isize * 10) + (decimal as isize))*(if is_negative {-1} else {1});

    Some(
        (
            the_name,
            rest
            )
    )
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let filepath: PathBuf = PathBuf::from(&args[1]);
    let file: File = File::open(filepath).unwrap();
    let mut map: HashMap<Box<[u8]>, (usize, isize, isize,isize)> = HashMap::new();
    let mut reader = MmapReader::new(file).bufreader();
    while let Some((station_name, measurement_times_ten)) = take_tokens(&mut reader) {

        if let Some(value) = map.get_mut(&station_name) {
            value.0 += 1;
            value.1 += measurement_times_ten;
            value.2 = std::cmp::min(measurement_times_ten, value.2);
            value.3 = std::cmp::max(measurement_times_ten, value.3);
        } else {
            map.insert(station_name.clone(), (1, measurement_times_ten, measurement_times_ten, measurement_times_ten));
        }
        // eprintln!("{} - {} -> {:?}", std::str::from_utf8(&station_name).unwrap(),measurement_times_ten, map[&station_name]);
    }
    print!("{{");
    let mut stations:Vec<Box<[u8]>> = map.keys().cloned().collect();
    stations.sort();

    for station in stations {
        let (count, sum, min, max) = map.get(&station).unwrap();
        let mean: f32 = (((*sum as f32)/(*count as f32) + 0.49))/10.0;
        let min:f32 = (*min as f32 + 0.49)/10.0;
        let max:f32 = (*max as f32 + 0.49)/10.0;
        let station:&str = core::str::from_utf8(&station).unwrap();
        print!("{station}={min:.1}/{mean:.1}/{max:.1}, ")
    }
    print!("}}");
}
