#!/usr/bin/python3
import sys
from collections import defaultdict
from sys import argv
import io
import time
import datetime
def parse_row(row: bytes):
    ba = bytearray(row)
    end = len(ba)-1
    rfound = ba.rfind(b';',2,end)
    return row[:rfound], float(ba[rfound+1])


def format(value: float):
    return f"{value + 0.05:.1f}"


def parse_csv(filename):
    try:
        aggregate = dict()
        with open(filename,'rb', 1_000_000_000) as fd:
            start_time = time.time()
            fd.seek(0, io.SEEK_END)
            total_filesize = fd.tell()
            fd.seek(0)
            next_percent = 0
            for (idx, line) in enumerate(iter(fd.readline, '')):
                (station_name, measurement) = parse_row(line)
                try:
                    elements = aggregate[station_name][:]
                    elements[0] += 1
                    elements[1] += measurement
                    if elements[2] > measurement:
                        elements[2] = measurement
                    elif elements[3] < measurement:
                        elements[3] = measurement
                    if idx % 1_000_000 == 0:
                        progress = fd.tell()/total_filesize
                        if progress > next_percent:
                            seconds_elapsed = time.time()-start_time
                            next_percent = progress + 0.001
                            print(f"progress -> {100*progress:.2f}%", file=sys.stderr)
                            print(f"elapsed: {seconds_elapsed} s", file=sys.stderr)
                            print(f"end_estimate: {datetime.timedelta(seconds=seconds_elapsed/progress)}", file=sys.stderr)
                except KeyError:
                    aggregate[station_name] = [1, measurement, measurement, measurement]
        return aggregate
    except KeyboardInterrupt as e:
        print("Exiting early due to user interruption",file=sys.stderr)



if __name__ == "__main__":
    filename = argv[1]
    import cProfile
    cProfile.run('parse_csv(filename)')
    sys.exit(0)
    stations = sorted(aggregate.values())
    print("{", end="")
    for station_name in stations:
        [station_count, station_sum, station_min, station_max] = aggregate[station_name]
        station_mean = station_sum / station_max
        print(f"{station_name}={format(station_min)}/{format(station_mean)}/{format(station_max)}, ", end="")
    print("}")
