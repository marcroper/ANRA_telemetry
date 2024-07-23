import csv
import os
import asyncio
import sys


# MR
# Function to simulate the transfer of the matlab generated telemetry from the high fidelity simulation (SHEPHERD)
# Arrival rate is around once per second
# For testing purposes only (not required if SHEPHERD simulation is running).
# Parameters are
# - source file (Simulated source of SHEPHERD data - this will normally be generated directly from the Matlab code)
# - destination file (where the SHEPHERD data should be written to for the send_telemetry code to read from)
async def simulate_telemetry_arriving_from_SHEPHERD(source_file, destination_file):
    print('Reading SHEPHERD data from file ...')
    with open(source_file, 'r', newline='') as data_source:  # Simulated source of SHEPHERD data - this will normally be generated directly from the Matlab code
        shepherd_reader = csv.reader(data_source)
        with open(destination_file, 'a+', newline='') as data_dest:  # SHEPHERD would be writing directly to this file around once per second and treats it as a csv file.
            shepherd_writer = csv.writer(data_dest)
            for next_line in shepherd_reader:
                shepherd_writer.writerow(next_line)
                # Uncomment below to check on file creation
                # file_stats = os.stat(destination_file)
                # print(f'IN LOOP: File Size in Bytes is {file_stats.st_size}')
                data_dest.flush()
                await asyncio.sleep(1.0)
        print('End of SHEPHERD data')
        data_source.close()
        data_dest.close()


async def main():
    args = sys.argv[1:]
    if len(args) != 2:
        sys.exit("Wrong number of arguments - Please provide names of source and destination files")
    source_file = args[0]
    destination_file = args[1]
    task0 = asyncio.create_task(simulate_telemetry_arriving_from_SHEPHERD(source_file, destination_file))
    await asyncio.gather(task0)
    print(f"SHEPHERD data transmission complete")

# If using this the name of the SHEPHERD source file and the destination file ned to be provided as arguments

asyncio.run(main())