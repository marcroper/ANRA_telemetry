from __future__ import print_function
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2

import grpc
import Telemetry_pb2
import Telemetry_pb2_grpc
import requests
import time
import csv


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

    def sendTelemetry(self, file_path):
        access_token = self.get_access_token('admin@dis.test', 'Password2')
        headers = (('authorization', access_token),)
        file_data = self.read_file(file_path)

        print(access_token)

        payload = Payload()
        payload.update(file_data)

        channel = grpc.insecure_channel('utm-delivery-grpc-telem.smartskies.io:1605')
        stub = Telemetry_pb2_grpc.TelemetryModuleStub(channel)

        stub.PostTelemetry(payload, metadata=headers)
        print(1)

    def read_file_line(self, file_path, line_number):
        data = []

        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            for i, row in enumerate(reader):
                if i == line_number:
                    point_tuple = (row[4][2:].strip(), float(row[5]), float(row[6][:-1]))
                    line_data = [
                        row[0],
                        int(row[1]),
                        int(row[2]),
                        row[3],
                        point_tuple,  # Extracting values inside parentheses
                        float(row[7]),
                        int(row[8]),
                        int(row[9]),
                        int(row[10]),
                        row[11],
                        float(row[12]),
                        int(row[13]),
                        float(row[14]),
                        float(row[15]),
                        float(row[16]),
                        float(row[17]),
                        float(row[18]),
                        float(row[19]),
                        float(row[20]),
                        float(row[21]),
                        float(row[22])
                    ]
                    return line_data

        return None

    def read_file(self, file_path):
        data = []

        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                if row:
                    point_tuple = (row[4][2:].strip(), float(row[5]), float(row[6][:-1]))
                    line_data = {
                        'operation_id': row[0],
                        'enroute_positions_id': int(row[1]),
                        'registration_id': int(row[2]),
                        'reference_number': row[3],
                        'location': point_tuple,  # Extracting values inside parentheses
                        'ground_speed_kt': float(row[7]),
                        'time_measured': int(row[8]),
                        'time_send': int(row[9]),
                        'battery_remaining': int(row[10]),
                        'mode': row[11],
                        'altitude_ft_wgs84': float(row[12]),
                        'altitude_num_gps_satellites': int(row[13]),
                        'hdop_gps': float(row[14]),
                        'track_bearing_deg': float(row[15]),
                        'track_bearing_reference': float(row[16]),
                        'vdop_gps': float(row[17]),
                        'roll': float(row[18]),
                        'yaw': float(row[19]),
                        'pitch': float(row[20]),
                        'climbrate': float(row[21]),
                        'heading': float(row[22])
                    }
                    data.append(line_data)

        return data


class Payload:
    enroute_id = ()
    payload = ()

    def __int__(self, file_data):

        for line_data in file_data:
            Telemetry_pb2.Telemetry(operation_id=line_data['operation_id'],
                                enroute_positions_id=line_data['enroute_positions_id'],
                                registration_id=line_data['registration_id'],
                                reference_number=line_data['reference_number'],
                                location=Telemetry_pb2.Telemetry.PointModel(type=line_data['location'][0],
                                                                            lat=line_data['location'][1],
                                                                            lng=line_data['location'][2]),
                                ground_speed_kt=line_data['ground_speed_kt'], time_measured=line_data['time_measured'],
                                time_send=line_data['time_send'], battery_remaining=line_data['battery_remaining'],
                                mode=line_data['mode'], altitude_ft_wgs84=line_data['altitude_ft_wgs84'],
                                altitude_num_gps_satellites=line_data['altitude_num_gps_satellites'],
                                hdop_gps=line_data['hdop_gps'], track_bearing_deg=line_data['track_bearing_deg'],
                                track_bearing_reference=line_data['track_bearing_reference'],
                                vdop_gps=line_data['vdop_gps'], roll=line_data['roll'], yaw=line_data['yaw'],
                                pitch=line_data['pitch'], climbrate=line_data['climbrate'],
                                heading=line_data['heading'])

    def __iter__(self):
        return self

    def update(self, next_payload):
        self.payload = next_payload

    def __next__(self):
        time.sleep(1)
        return self.payload


def main():
    client_id = 'DMS'

    oauth_client = ANRAOAuthClient(client_id)

    line_number = 0
    while line_number < 120:
        latest_line_data = oauth_client.read_file_line('ANRA_telemetry120sOLD.txt', line_number=line_number)
        if latest_line_data:
            with open('test.txt', 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([
                    latest_line_data[0], latest_line_data[1], latest_line_data[2],
                    latest_line_data[3], *latest_line_data[4],
                    latest_line_data[5], latest_line_data[6], latest_line_data[7],
                    latest_line_data[8], latest_line_data[9],
                    latest_line_data[10], latest_line_data[11], latest_line_data[12],
                    latest_line_data[13], latest_line_data[14], latest_line_data[15],
                    latest_line_data[16], latest_line_data[17], latest_line_data[18],
                    latest_line_data[19], latest_line_data[20],
                ])
        # file_data = oauth_client.read_file('test.txt')
        oauth_client.sendTelemetry('test.txt')

        # Access the data
        # for line_data in file_data:
        #     print(line_data['operation_id'])

        line_number += 1
        time.sleep(1)


if __name__ == "__main__":
    main()
