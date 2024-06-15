import sys, os

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../proto')

import store_pb2 as store_pb2
import store_pb2_grpc as store_pb2_grpc
from concurrent import futures
import random
import time
import grpc

class MasterKeyValueStoreServicer(store_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        self.slave = {}
        self.delay = 0
        self.was_slowed = False
        super().__init__()

    def put(self, request, context):
        if not self.was_slowed:
            try:
                if len(self.slave) == 0:
                    # print(f"[MASTER] No slave available")
                    return store_pb2.PutResponse(success=False)
                # print(f"[MASTER] preparing to put '{request.key}' : '{request.value}'")
                key = request.key
                value = request.value

                prepare_responses = []
                commit_responses = []
                abort = False

                for adress, stub in self.slave.items():
                    prepare_request = store_pb2.PrepareRequest(key=key, value=value)
                    prepare_response = stub.prepare(prepare_request)
                    prepare_responses.append(prepare_response)

                if all(response.success for response in prepare_responses):
                    commit_request = store_pb2.CommitRequest(key=key)
                    # print(f"[MASTER] Committing...")
                    for stub in self.slave.values():
                        commit_responses.append(stub.commit(commit_request))
                else: 
                    abort = True
                    
                if abort and not all(response.success for response in prepare_responses):
                    abort_request = store_pb2.AbortRequest(key=key)
                    # print(f"[MASTER] Aborting prepare...")
                    for stub in self.slave.values():
                        stub.abort(abort_request)

                if not abort:
                    # print(f"[MASTER] Put success")
                    return store_pb2.PutResponse(success=True)
                else:
                    # print(f"[MASTER] Put failed")
                    return store_pb2.PutResponse(success=False)
                
            except Exception as e:
                # print(f"[MASTER] Put error")
                # print(f"[MASTER]",e)
                return store_pb2.PutResponse(success=False)
        else:
            # print(f"[MASTER] Put failed, was slowed down")
            return store_pb2.PutResponse(success=False)

    def get(self, request, context):
        if not self.was_slowed:
            try:
                if len(self.slave) == 0:
                    # print(f"[MASTER] No slaves available")
                    return store_pb2.GetResponse(found=False)
                else:
                    # print(f"[MASTER] Getting value from random slave")
                    for slave in list(self.slave.values()):
                        response = slave.get(request)
                        if response.found:
                            # print(f"[MASTER] Value found: '{response.value}' for key '{request.key}")
                            return response
                    return store_pb2.GetResponse(found=False)       
            except Exception as e:
                # print(f"[MASTER] Get error")
                # print(f"[MASTER]",e)
                return store_pb2.GetResponse(found=False)
        else:
            # print(f"[MASTER] Get failed, was slowed down")
            return store_pb2.GetResponse(found=False)

    def slowDown(self, request, context):
        # print(f"[MASTER] Slowing down for {request.seconds} seconds")
        self.delay = request.seconds
        self.was_slowed = True
        while self.delay > 0:
            time.sleep(1)
            self.delay -= 1
        # print(f"[MASTER] SlowDown terminated")
        return store_pb2.SlowDownResponse(success=True)

    def restore(self, request, context):
        if self.was_slowed:
            # print(f"[MASTER] Restoring data")
            self.delay = 0
            self.was_slowed = False
            return store_pb2.RestoreResponse(success=True)
        else:
            # print(f"[MASTER] Was not slowed")
            return store_pb2.RestoreResponse(success=True)

    def register(self, request, context):
        if not self.was_slowed:
            try:
                # print(f"[MASTER] Registering slave {request.ip}:{request.port}")
                channel = grpc.insecure_channel(f"{request.ip}:{request.port}")
                self.slave[f"{request.ip}:{request.port}"] = store_pb2_grpc.KeyValueStoreStub(channel)
                return store_pb2.RegisterResponse(success=True)
            
            except Exception as e:
                # print(f"[MASTER] Error connecting to slave {request.ip}:{request.port}")
                # print(f"[MASTER]",e)
                return store_pb2.RegisterResponse(success=False)
        else:
            # print(f"[MASTER] Register failed, was slowed down")
            return store_pb2.RegisterResponse(success=False)

    def unregister(self, request, context):
        if not self.was_slowed:
            try:
                # print(f"[MASTER] Unregistering slave {request.ip}:{request.port}")
                del self.slave[f"{request.ip}:{request.port}"]

            except Exception as e:
                # print(f"[MASTER] Error unregistering slave {request.ip}:{request.port}")
                # print(f"[MASTER]",e)
                return store_pb2.RegisterResponse(success=False)
        else: 
            # print(f"[MASTER] Unregister failed, was slowed down")
            return store_pb2.RegisterResponse(success=False)

def serve(ip, port):
    
    print(f"[MASTER] Serve at {ip}:{port}")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(MasterKeyValueStoreServicer(), server)
    server.add_insecure_port(f'{ip}:{port}') # Modificar por YAML
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve("localhost", 50051)
