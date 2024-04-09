
use std::fs::File;
use std::path::PathBuf;
use itertools::Itertools;
use rayon::prelude::*;
use halfbrown::HashMap;
// use hashbrown::HashMap;

use fasthash::{FastHash, FastHasher, StreamHasher};

// use std::collections::HashMap;
// use ahash::AHashMap;

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

    fn as_slice(&self) -> &[u8] {
        &self.map
    }
}

fn partition(slice:&[u8], n_readers:usize) -> Vec<&[u8]> {
    // Find boundaries
    let total_file_length = slice.len();
    eprintln!("Total file length : {}", total_file_length);
    let mut boundaries = vec![0; n_readers];
    // First boundary has to be 0
    boundaries[0] = 0;
    // Last boundary has to be file length;
    boundaries[n_readers - 1] = total_file_length;

    // Subsequent boundaries have to go in divisions of file length
    let division_size = total_file_length / (n_readers - 1);
    unsafe {
    for idx in 1..n_readers - 1 {
        let candidate = division_size * idx;
        boundaries[idx] = candidate + 1 + slice.get_unchecked(candidate..)
            .iter().position(|b| *b == b'\n')
            .expect("Boundary is required to be");
    }
    }
    // eprintln!("Boundaries: {:?}", boundaries);
    boundaries.into_iter()
        .tuple_windows::<(usize, usize)>()
        .map(|(start, end)| &slice[start..end]).collect_vec()
}


fn parse( line:&[u8]) -> (&[u8], isize){
    let read = line.len();
    // SAFETY: We are assuming, as the challenge tells us, that there is indeed a decimal before the line ends
    unsafe {
        // Take the decimal
        // eprintln!("{} count {}",std::str::from_utf8(&line).unwrap().trim(), read);
        let decimal = (line.get_unchecked(read-1)-48) as isize; // Correct
        let units = 10*(line.get_unchecked(read-3)-48) as isize;

        match line.get_unchecked(read-4) {
            b';' => {
                let location= line.get_unchecked(..read-4);
                let number = decimal + units;
                (location,number)

            },
            b'-' => {
                let location= line.get_unchecked(..read-5);
                let number = -(decimal + units);
                (location,number)
            },
            tens => {

                let tens = ((*tens-48) as isize)*100;
                match line.get_unchecked(read-5){
                b';' => {
                    let location= line.get_unchecked(..read-5);
                    let number = decimal + units + tens;
                    (location,number)

                },
                b'-' => {
                    let location= line.get_unchecked(..read-6);
                    let number = -(decimal + units + tens);
                    (location,number)
                },
                _ => {
                    unreachable!();
                }
            }}

        }
    }
}

fn aggregate_stations(mmap:&[u8]) -> HashMap<Box<[u8]>, (usize, isize, isize,isize)> {
    let mut map: HashMap<Box<[u8]>, (usize, isize, isize,isize)> = Default::default();
    let mut working_rest = mmap;
    while let Some(bytes_read) = working_rest.iter().position(|c| *c == b'\n') {

        let line = unsafe {working_rest.get_unchecked(0..bytes_read)};
        let (station_name, measurement_times_ten) = parse(line);
        working_rest = unsafe {working_rest.get_unchecked(bytes_read+1..)};
        if let Some(value) = map.get_mut(station_name) {
            value.0 += 1;
            value.1 += measurement_times_ten;
            if measurement_times_ten < value.2 {
                value.2 = measurement_times_ten;
            }
            if measurement_times_ten < value.3 {
                value.3 = measurement_times_ten;
            }
        } else {
            map.insert(Box::from(station_name), (1, measurement_times_ten, measurement_times_ten, measurement_times_ten));
        }
        // eprintln!("{} - {} -> {:?}", std::str::from_utf8(&station_name).unwrap(),measurement_times_ten, map[&station_name]);
    }
    //eprintln!("Hashbrown length -> {}",map.len());
    map
}


fn main() {
    let args: Vec<String> = std::env::args().collect();
    let filepath: PathBuf = PathBuf::from(&args[1]);
    let file: File = File::open(filepath).unwrap();
    let map = Mmap::new(file);
    let readers = partition(map.as_slice(),16);
    rayon::ThreadPoolBuilder::new().num_threads(16).build_global().unwrap();
    let _hashmaps: Vec< HashMap<Box<[u8]>, (usize, isize, isize,isize)> > = readers
            .par_iter()
        .map(|mmap_reader| aggregate_stations(mmap_reader))
        .collect();
    // println!("{:?}", _hashmaps)
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
