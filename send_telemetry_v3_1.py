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
        return ""


class ANRAOAuthClient:
    base_auth_endpoint = f'https://oauth.flyanra.net'
    auth_endpoint = f'{base_auth_endpoint}/auth/realms/ANRA/protocol/openid-connect/token'
    refresh_token_endpoint = f'{base_auth_endpoint}/auth/realms/ANRA/protocol/openid-connect/token'

    def __init__(self, client_id):
        self.client_id = client_id
        self.token_url = "https://oauth.flyanra.net/auth/realms/ANRA/protocol/openid-connect/token"

    def get_access_token(self, username, password):
        token_payload = {
            "grant_type": 'password',
            "client_id": self.client_id,
            "username": username,
            "password": password
        }

        response = requests.post(self.token_url, data=token_payload)

        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            return access_token
        else:
            print(response.status_code)
            return None

class GrpcClient:        
    def __init__(self):        
        print('GrpcClient init')
    
        print("Getting Token from oauth")
        client_id = 'DMS'
        oauth_client = ANRAOAuthClient(client_id)
        access_token = oauth_client.get_access_token('admin@dis.test', 'Password2')
        print(access_token)
        self.oauth_token = access_token

        self.server_dns = 'utm-delivery-grpc-telem.smartskies.io' #remote ip
        self.port = '1605' #remote port

        start_time = time.time()

    def __iter__(self):
        return self

    def __next__(self):
        print(f"Start the next telemetry")
        result = read_line_from_queue()
        if result != "":
            self.set_telemetry(result)
            # time.sleep(1)
            return self.pack_telemetry()
        else:
            raise StopIteration


    def set_telemetry(self, row):
        point_tuple = (row[4].strip(), float(row[5]), float(row[6][:-1]))
        line_data = {
            "operation_id": row[0],
            "enroute_positions_id": row[1],
            "registration_id": row[2].strip(),
            "reference_number": row[3].strip(),
            "location": point_tuple,  # Extracting values inside parentheses
            "ground_speed_kt": float(row[7]),
            "time_measured": int(row[8]),
            "time_send": int(row[9]),
            "battery_remaining": int(row[10]),
            "mode": row[11],
            "altitude_ft_wgs84": float(row[12]),
            "altitude_num_gps_satellites": int(row[13]),
            "hdop_gps": float(row[14]),
            "track_bearing_deg": float(row[15]),
            "track_bearing_reference": float(row[16]),
            "vdop_gps": float(row[17]),
            "roll": float(row[18]),
            "yaw": float(row[19]),
            "pitch": float(row[20]),
            "climbrate": float(row[21]),
            "heading": float(row[22])  # CSV file do not have data for index 22
        }
        print(f'Telemetry data : {line_data}')
        self.aircraft = line_data

    def pack_telemetry(self):
        telemetry = Telemetry_pb2.Telemetry()
        telemetry.operation_id = self.aircraft["operation_id"]
        telemetry.enroute_positions_id = str(uuid.uuid4())
        telemetry.registration_id = self.aircraft["registration_id"]
        telemetry.reference_number = self.aircraft["reference_number"]
        telemetry.location.type = 'Point'
        telemetry.location.lat = self.aircraft["location"][1]
        telemetry.location.lng = self.aircraft["location"][2]
        telemetry.ground_speed_kt = self.aircraft["ground_speed_kt"]
        telemetry.time_measured = self.aircraft["time_measured"]
        telemetry.time_send = self.aircraft["time_send"]
        telemetry.battery_remaining = self.aircraft["battery_remaining"]
        telemetry.mode = 'MODE_AUTO'
        telemetry.altitude_ft_wgs84 = self.aircraft["altitude_ft_wgs84"]
        telemetry.altitude_num_gps_satellites = self.aircraft["altitude_num_gps_satellites"]
        telemetry.hdop_gps =self.aircraft["hdop_gps"]
        telemetry.track_bearing_deg = self.aircraft["track_bearing_deg"]
        telemetry.track_bearing_reference = Telemetry_pb2.Telemetry.MAGNETIC_NORTH
        telemetry.vdop_gps = self.aircraft["vdop_gps"]
        telemetry.roll = self.aircraft["roll"]
        telemetry.yaw = self.aircraft["yaw"]
        telemetry.pitch = self.aircraft["pitch"]
        telemetry.climbrate = self.aircraft["climbrate"]
        telemetry.heading = self.aircraft["heading"]
        print (f'Telemetry lat lng is {telemetry.location.lat} {telemetry.location.lng} and heading is {telemetry.heading}')
        return telemetry

    async def post_telemetry(self):
        try:                
            # Create an insecure channel
            msg_channel = ''.join([self.server_dns,':',self.port])
            channel = grpc.insecure_channel(msg_channel)

            # Create a stub for transmission on the channel
            self.stub = Telemetry_pb2_grpc.TelemetryModuleStub(channel)

            print("Telemetry Post")
            response = self.stub.PostTelemetry(self, metadata=(('authorization', self.oauth_token),))

            if response:
                print("Telemetry Post is Ended:", response)

        except Exception as e:
            print(f'Error in telemetry push ({e})')

        except grpc.RpcError as rpc_error:
            print(rpc_error.code())

        else:
            print(grpc.StatusCode.OK)

    def close(self):
        print("Self Closed")
        self.channel.close()

async def main():
    g = GrpcClient()

    telemetry_file = 'ANRA_telemetry.txt'  # Telemtry data for onward transmission
    task0 = asyncio.create_task(simulate_telemetry_arriving_from_SHEPHERD(telemetry_file))

    time.sleep(2.0)  # give it a chance to get going...

    taskPopulateQ = asyncio.create_task(populate_queue(telemetry_file))
    taskPostTelemetry = asyncio.create_task(g.post_telemetry())

    await asyncio.gather(task0, taskPopulateQ, taskPostTelemetry)

    print(f"Telemetry data transmission complete")

    
asyncio.run(main())