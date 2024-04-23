import requests 
import time
import Telemetry_pb2_grpc, Telemetry_pb2

import grpc
import uuid
import math
import signal
import csv
import queue
from collections import deque
import os
import asyncio


# Modifications made by MR to handle the fact that the file is a live stream of data from the high fidelity Matlab simulation

# queue for telemetry
telem_q = queue.Queue()


# MR
# Function to simulate the transfer of the matlab generated telemetry from the high fidelity simulation (SHEPHERD)
# Arrival rate is around once per second
# For testing purposes only (not required if SHEPHERD simulation is running). Please do not modify
async def simulate_telemetry_arriving_from_SHEPHERD(telemetry_file):
    print('Reading SHEPHERD data from file ...')
    with open('ANRA_telemetry_source.txt', 'r',  newline='') as data_source:  # Simulated source of SHEPHERD data - this will normally be generated directly from the Matlab code
        shepherd_reader = csv.reader(data_source)
        with open(telemetry_file, 'a+', newline='') as data_dest: # SHEPHERD would be writing directly to this file around once per second and treats it as a csv file.
            shepherd_writer = csv.writer(data_dest)
            for next_line in shepherd_reader:
                shepherd_writer.writerow(next_line)
                data_dest.flush()
                await asyncio.sleep(1.0)
        print('End of SHEPHERD data')
        data_source.close()



# Reads the next line of telemetry data from the file and adds it to the queue.
async def add_next_line_to_queue(telemetry_source):
    line = telemetry_source.readline()
    count =0
    while not line and count < 20: # Give it 10 seconds to allow any more data to arrive
        await asyncio.sleep(0.5) # wait for data to arrive
        line = telemetry_source.readline() # and try again
        count += 1
    if line:
        telem_q.put(line)
        # Signal line added to queue
        return 1
    else:
        # No more data to read from file
        return 0

# Build up queue of telemetry data from the telemetry file
async def populate_queue(telemetry_file):
    telemetry_source = open(telemetry_file, 'r', newline='')
    while True:
        result = await add_next_line_to_queue(telemetry_source)
        if not result:
            # No more data to add
            telemetry_source.close()
            break

# Read one line from the queue
# If nothing arrives for 10 seconds then assume that is the end of the data.
# async def read_line_from_queue():
#     qcount = 0
#     while (telem_q.qsize() <= 0 and qcount < 20):
#         await asyncio.sleep(0.5)
#         qcount += 1
#     if (telem_q.qsize() > 0):
#         # There's data there so return it
#         qcontents = telem_q.get()
#         return qcontents
#     else:
#         # No more data to read from queue
#         return 0

# Changed from async to accommodate the fact that this is called from the grpc client next function (and process queue will not exist)
def read_line_from_queue():
    qcount = 0
    while (telem_q.qsize() <= 0 and qcount < 20):
        time.sleep(0.5)
        qcount += 1
    if (telem_q.qsize() > 0):
        # There's data there so return it
        qcontents = telem_q.get()
        return qcontents
    else:
        # No more data to read from queue
        return 0


# Read from the queue until there is nothing left
async def process_queue():
    while True:
        result = read_line_from_queue()
        if not result:
            break

async def main():
    # g = GrpcClient()
    telemetry_file = 'ANRA_telemetry.txt' # Telemtry data for onward transmission
    task0 = asyncio.create_task(simulate_telemetry_arriving_from_SHEPHERD(telemetry_file))

    time.sleep(2.0) # give it a chance to get going...

    taskPopulateQ = asyncio.create_task(populate_queue(telemetry_file))
    taskProcessQ = asyncio.create_task(process_queue())

    await asyncio.gather(task0, taskPopulateQ, taskProcessQ)

    print(f"Telemetry data transmission complete")
    
asyncio.run(main())