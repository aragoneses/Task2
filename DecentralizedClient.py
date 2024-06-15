import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/proto')

import yaml
import grpc
import store_pb2 as store_pb2
import store_pb2_grpc as store_pb2_grpc

def put(key, value):
    request = store_pb2.PutRequest(key=key, value=value)
    response = stub[node].put(request)
    return response.success

def get(key):
    request = store_pb2.GetRequest(key=key)
    response = stub[node].get(request)
    return response.value, response.found

def slow_down(seconds):
    request = store_pb2.SlowDownRequest(seconds=seconds)
    response = stub[node].slowDown(request)
    return response.success

def restore():
    request = store_pb2.RestoreRequest()
    response = stub[node].restore(request)
    return response.success

with open('decentralized_config.yaml', 'r') as file:
    config = yaml.safe_load(file)

node_configs = config["nodes"]
slaves = []

stub = []
total = 0
for node_config in node_configs:
    stub.append(store_pb2_grpc.KeyValueStoreStub(grpc.insecure_channel(f'{node_config["ip"]}:{node_config["port"]}')))
    total += 1

while True:
    nocont = False
    while not nocont:
        try:
            node = int(input(f"Enter node ( 0 - {total - 1} )\n"))
            if node < 0 or node >= total:
                print("Invalid node")
            else:
                nocont = True
        except Exception:
            pass
    print("COMMNADS:")
    print("PUT key value")
    print("GET key")
    print("SLOW seconds")
    print("RESTORE")
    print("EXIT")
    command = input()
    os.system('cls')
    command_split = command.split(" ")
    command_split[0] = command_split[0].upper()
    if command_split[0] == 'GET':
        if len(command_split) == 2:
            key = command_split[1]
            value, found = get(key)
            if found:
                print("Value:", value)
            else:
                print("Not found")
        else:
            print("Usage: GET 'key'")
    elif command_split[0] == 'PUT':
        if len(command_split) == 3:
            key = command_split[1]
            value = command_split[2]
            success = put(key, value)
            if success:
                print("Success")
            else:
                print("Failed")
        else:
            print("Usage: PUT 'key' 'value'")
    elif command_split[0] == 'SLOW':
        if len(command_split) == 2:
            seconds = int(command_split[1])
            success = slow_down(seconds)
            if success:
                print("Success")
            else:
                print("Failed")
        else:
            print("Usage: SLOW 'seconds'")
    elif command_split[0] == 'RESTORE':
        if len(command_split) == 1:
            success = restore()
            if success:
                print("Success")
            else:
                print("Failed")
        else:
            print("Usage: RESTORE")
    elif command_split[0] == 'EXIT':
        print("Exiting....")
        break
    else:
        print("Invalid command")
    print()
    print("------------------------------------------------")
    print()