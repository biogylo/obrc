use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::sync::Arc;
use itertools::Itertools;
use rayon::prelude::*;

#[derive(Debug)]
struct Mmap {
    map:memmap2::Mmap,
    _file:File
}
impl Mmap {
    fn new(file:File) -> Mmap {
        let map =unsafe {
            // Safety: this is a read only map
            memmap2::Mmap::map(&file).expect("Unable to mmap file!")
        };
        Mmap {
            map,
            _file:file,
        }
    }
}
#[derive(Clone, Debug)]
pub struct MmapReader {
    map: Arc<Mmap>,
    start: usize,
    offset: usize,
    end: usize
}

impl Read for MmapReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let actual_start = self.start + self.offset;
        let bytes_left = self.end-actual_start;
        let bytes_read = std::cmp::min(bytes_left, buf.len());
        let map_slice = &self.map.map[actual_start..actual_start+bytes_read];
        let buf_slice = &mut buf[..bytes_read];
        buf_slice.copy_from_slice(map_slice);
        self.offset += bytes_read;
        Ok(bytes_read)
    }
}

impl MmapReader {
    fn partition(file:File, n_readers:usize, boundary_char:u8) -> Vec<MmapReader> {
        assert!(n_readers >= 2);
        let map = Mmap::new(file);
        // Find boundaries
        let total_file_length = map.map.len();
        eprintln!("Total file length : {}", total_file_length);
        let mut boundaries = vec![0;n_readers];
        // First boundary has to be 0
        boundaries[0] = 0;
        // Last boundary has to be file length;
        boundaries[n_readers-1] = total_file_length;

        // Subsequent boundaries have to go in divisions of file length
        let division_size = total_file_length/(n_readers-1);
        for idx in 1..n_readers-1 {
            let candidate = division_size*idx;
            boundaries[idx] = candidate + 1 + map.map[candidate..].iter().position(|b| *b == boundary_char).expect("Boundary is required to be");
        }
        eprintln!("Boundaries: {:?}", boundaries);
        let rc_map = Arc::new(map);
        boundaries.into_iter()
            .tuple_windows::<(usize,usize)>()
            .map(|(start, end)|
                MmapReader {
                    map:rc_map.clone(),
                    start,
                    offset:0,
                    end,
                }
            ).collect_vec()
    }
    fn bufreader(self) -> BufReader<MmapReader> {
        BufReader::new(self)
    }
    fn _new(file: File) -> MmapReader {
        let mmap = Mmap::new(file);
        let end = mmap.map.len();
        let map = Arc::new(mmap);
        let offset = 0;
        let start = 0;
        MmapReader{
            map,
            start,
            offset,
            end
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

fn aggregate_stations(mmap_reader:MmapReader) -> HashMap<Box<[u8]>, (usize, isize, isize,isize)> {
    let mut map: HashMap<Box<[u8]>, (usize, isize, isize,isize)> = HashMap::new();
    let mut reader = mmap_reader.bufreader();
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
    map
}


fn main() {
    let args: Vec<String> = std::env::args().collect();
    let filepath: PathBuf = PathBuf::from(&args[1]);
    let file: File = File::open(filepath).unwrap();
    let readers = MmapReader::partition(file,16,10);
    let hashmaps: Vec<HashMap<Box<[u8]>,(usize,isize,isize,isize)>> = readers.into_par_iter().map(|mmap_reader| aggregate_stations(mmap_reader)).collect();
    println!("{:?}", hashmaps)
    // print!("{{");
    // let mut stations:Vec<Box<[u8]>> = map.keys().cloned().collect();
    // stations.sort();
    // for station in stations {
    //     let (count, sum, min, max) = map.get(&station).unwrap();
    //     let mean: f32 = (((*sum as f32)/(*count as f32) + 0.49))/10.0;
    //     let min:f32 = (*min as f32 + 0.49)/10.0;
    //     let max:f32 = (*max as f32 + 0.49)/10.0;
    //     let station:&str = core::str::from_utf8(&station).unwrap();
    //     print!("{station}={min:.1}/{mean:.1}/{max:.1}, ")
    // }
    // print!("}}");
}
