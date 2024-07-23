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
import linecache
from collections import deque
import sys


def get_line(filename, n):
    with open(filename, 'r') as f:
        suma = sum(1 for line in f)
        f.seek(0, 0)
        for index_number, line in enumerate(f):
            if index_number + 1 == n:
                return line


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

    # source_telemetry_file = 'ANRA_telemetry_source.txt'
    # telemetry_file = 'ANRA_telemetry.txt'  # Telemetry data for onward transmission

    def __init__(self, telemetry_source):
        print('GrpcClient init')
        self.telemetry_source_file = telemetry_source
        # print(f"(init)Telemetry file is: {self.telemetry_source_file}")
        print("Getting Token from oauth")
        client_id = 'DMS'
        oauth_client = ANRAOAuthClient(client_id)
        access_token = oauth_client.get_access_token('admin@dis.test', 'Password2')
        print(access_token)
        self.oauth_token = access_token

        self.server_dns = 'utm-delivery-grpc-telem.smartskies.io'  # remote ip
        self.port = '1605'  # remote port

        start_time = time.time()

        self.currentIndex = 1

    def __iter__(self):
        return self

    def __next__(self):
        print(f"Start the next telemetry")
        newIncomingLineCount = 0
        poll_count = 0
        # source_file = "ANRA_telemetry_source.txt"
        source_file = self.telemetry_source_file
        # print(f"(next)Telemetry file is: {source_file}")
        with open(source_file, 'r', newline='') as f:
            totalRows = sum(1 for line in f)
            while True:
                if totalRows >= self.currentIndex or newIncomingLineCount > 0:
                    if newIncomingLineCount > 0:
                        print(
                            f"Line total: {totalRows + newIncomingLineCount}, current row: {self.currentIndex}, new line count: {newIncomingLineCount}")
                        newIncomingLineCount = 0
                    else:
                        print(
                            f"Line total: {totalRows}, current row: {self.currentIndex}, new line count: {newIncomingLineCount}")
                    result = get_line(source_file, self.currentIndex)
                    self.currentIndex = self.currentIndex + 1
                    if result:
                        print(f"Start set telemetry : {result}")
                        self.set_telemetry(result.split(','))
                        # time.sleep(1)
                        return self.pack_telemetry()
                    else:
                        print(f"Skip empty row : {result}")
                else:
                    time.sleep(3)
                    newIncomingLineCount = sum(1 for line in f)
                    if newIncomingLineCount > 0:
                        continue
                    print(f'Polling file. Row changes count = {newIncomingLineCount}')
                    poll_count += 1
                    if poll_count > 10:
                        break

    def set_telemetry(self, row):
        point_tuple = ('Point', float(row[5]), float(row[6][:-1]))
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
        telemetry.hdop_gps = self.aircraft["hdop_gps"]
        telemetry.track_bearing_deg = self.aircraft["track_bearing_deg"]
        telemetry.track_bearing_reference = Telemetry_pb2.Telemetry.MAGNETIC_NORTH
        telemetry.vdop_gps = self.aircraft["vdop_gps"]
        telemetry.roll = self.aircraft["roll"]
        telemetry.yaw = self.aircraft["yaw"]
        telemetry.pitch = self.aircraft["pitch"]
        telemetry.climbrate = self.aircraft["climbrate"]
        telemetry.heading = self.aircraft["heading"]
        print(
            f'Telemetry lat lng is {telemetry.location.lat} {telemetry.location.lng} and heading is {telemetry.heading}')
        return telemetry

    def get_grpc_stream(self):
        print("get_grpc_stream is called")
        try:
            msg_channel = ''.join([self.server_dns, ':', self.port])
            channel = grpc.insecure_channel(msg_channel)
            self.stub = Telemetry_pb2_grpc.TelemetryModuleStub(channel)

            grpcStream = self.stub.PostTelemetry(self, metadata=(('authorization', self.oauth_token),))

            if grpcStream:
                print("get_grpc_stream:", grpcStream)
                return grpcStream

            print("get_grpc_stream is ended")
        except Exception as e:
            print(f'Error in get_grpc_stream ({e})')

        except grpc.RpcError as rpc_error:
            print(rpc_error.code())

        else:
            print(grpc.StatusCode.OK)

    def close(self):
        print("Self Closed")
        self.channel.close()

# async def main(args):
def main(args):
    if len(args) != 1:
        sys.exit("Wrong number of arguments - Please provide name of source file")
    thefile = args[0]
    print(f"Creating client")
    g = GrpcClient(thefile)
    g.get_grpc_stream()
    print(f"Telemetry data transmission complete")


# asyncio.run(main(sys.argv[1:]))
# Module requires the name of the telemetry source file to be provided as an argument
if __name__ == "__main__":
    main(sys.argv[1:])
