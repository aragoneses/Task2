import sys, os

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../proto')

import store_pb2 as store_pb2
import store_pb2_grpc as store_pb2_grpc
from concurrent import futures
import time
import grpc

class MasterKeyValueStoreServicer(store_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        self.slave = {}
        self.delay = 0
        self.was_slowed = False
        super().__init__()

    def get(self, request, context):
        if not self.was_slowed:
            try:
                if len(self.slave) == 0:
                    return store_pb2.GetResponse(found=False)
                else:
                    for slave in list(self.slave.values()):
                        response = slave.get(request)
                        if response.found:
                            return response
                    return store_pb2.GetResponse(found=False)       
            except Exception as e:
                return store_pb2.GetResponse(found=False)
        else:
            return store_pb2.GetResponse(found=False)

    def put(self, request, context):
        if not self.was_slowed:
            try:
                if len(self.slave) == 0:
                    return store_pb2.PutResponse(success=False)
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
                    for stub in self.slave.values():
                        commit_responses.append(stub.commit(commit_request))
                else: 
                    abort = True
                    
                if abort and not all(response.success for response in prepare_responses):
                    abort_request = store_pb2.AbortRequest(key=key)
                    for stub in self.slave.values():
                        stub.abort(abort_request)

                if not abort:
                    return store_pb2.PutResponse(success=True)
                else:
                    return store_pb2.PutResponse(success=False)
                
            except Exception as e:
                return store_pb2.PutResponse(success=False)
        else:
            return store_pb2.PutResponse(success=False)

    def register(self, request, context):
        if not self.was_slowed:
            try:
                channel = grpc.insecure_channel(f"{request.ip}:{request.port}")
                self.slave[f"{request.ip}:{request.port}"] = store_pb2_grpc.KeyValueStoreStub(channel)
                return store_pb2.RegisterResponse(success=True)
            
            except Exception as e:
                return store_pb2.RegisterResponse(success=False)
        else:
            return store_pb2.RegisterResponse(success=False)
        
    def restore(self, request, context):
        if self.was_slowed:
            self.delay = 0
            self.was_slowed = False
            return store_pb2.RestoreResponse(success=True)
        else:
            return store_pb2.RestoreResponse(success=True)

    def slowDown(self, request, context):
        self.delay = request.seconds
        self.was_slowed = True
        while self.delay > 0:
            time.sleep(1)
            self.delay -= 1
        return store_pb2.SlowDownResponse(success=True)

    def unregister(self, request, context):
        if not self.was_slowed:
            try:
                del self.slave[f"{request.ip}:{request.port}"]

            except Exception as e:
                return store_pb2.RegisterResponse(success=False)
        else: 
            return store_pb2.RegisterResponse(success=False)

def serve(ip, port):
    
    print(f"[MASTER] Serve at {ip}:{port}")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(MasterKeyValueStoreServicer(), server)
    server.add_insecure_port(f'{ip}:{port}') 
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve("localhost", 50051)
