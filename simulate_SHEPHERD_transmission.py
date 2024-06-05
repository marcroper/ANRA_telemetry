import requests 
import time
import Telemetry_pb2_grpc, Telemetry_pb2

import grpc
import uuid
import math
import csv
import queue
# from collections import deque
import os
import asyncio


# MR
# Function to simulate the transfer of the matlab generated telemetry from the high fidelity simulation (SHEPHERD)
# Arrival rate is around once per second
# For testing purposes only (not required if SHEPHERD simulation is running).
async def simulate_telemetry_arriving_from_SHEPHERD(telemetry_file):
    print('Reading SHEPHERD data from file ...')
    # with open('ANRA_telemetry_test.txt', 'r',  newline='') as data_source:  # Simulated source of SHEPHERD data - this will normally be generated directly from the Matlab code
    with open('ANRA_telemetry_trial.txt', 'r', newline='') as data_source:  # Simulated source of SHEPHERD data - this will normally be generated directly from the Matlab code

        shepherd_reader = csv.reader(data_source)
        with open(telemetry_file, 'a+', newline='') as data_dest: # SHEPHERD would be writing directly to this file around once per second and treats it as a csv file.
            shepherd_writer = csv.writer(data_dest)
            for next_line in shepherd_reader:
                shepherd_writer.writerow(next_line)
                # file_stats = os.stat('ANRA_telemetry_from_SHEP.txt')
                file_stats = os.stat('ANRA_telemetry_source.txt')
                print(f'IN LOOP: File Size in Bytes is {file_stats.st_size}')
                data_dest.flush()
                await asyncio.sleep(1.0)
        print('End of SHEPHERD data')
        data_source.close()


async def main():

    # telemetry_file = ('ANRA_telemetry_from_SHEP.txt')  # Telemetry data for onward transmission
    telemetry_file = ('ANRA_telemetry_source.txt')  # Telemetry data for onward transmission
    task0 = asyncio.create_task(simulate_telemetry_arriving_from_SHEPHERD(telemetry_file))

    await asyncio.gather(task0)

    print(f"SHEPHERD data transmission complete")

asyncio.run(main())