import sys, os

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../proto')

import grpc
import store_pb2 as store_pb2
import store_pb2_grpc as store_pb2_grpc
from concurrent import futures
import time

class NodeKeyValueStoreServicer(store_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, ip, port, node0_host, weight, node0_weight):
        self.data = {}
        self.temp = {}
        self.nodes = {}
        self.ip = ip
        self.port = port
        self.was_slowed = False
        self.weight = weight
        self.min_weight_for_commit = 0 
        self.min_weight_for_get = 0 
        self.timeout = 0.1
        self.delay = 0
        self.file = f"destorage.json"

        self.read_file(self.file)

        try:
            if f"{ip}:{port}" != node0_host: 
                channel = grpc.insecure_channel(f'{node0_host}')
                stub = store_pb2_grpc.KeyValueStoreStub(channel)
                response = stub.register(store_pb2.RegisterRequest(ip=self.ip, port=self.port, weight=self.weight), timeout=self.timeout)
                self.nodes[node0_host] = (stub, node0_weight)
                if response.success == False:
                    exit()
                else:
                    pass

                node_num = 0
                response_addcon = stub.addcon(store_pb2.AddConRequest(num=node_num), timeout=self.timeout)
                while response_addcon.node != '':
                    if response_addcon.node != f'{self.ip}:{self.port}':
                        try:
                            node_channel = grpc.insecure_channel(f'{response_addcon.node}')
                            node_stub = store_pb2_grpc.KeyValueStoreStub(node_channel)
                            node_response = node_stub.register(store_pb2.RegisterRequest(ip=self.ip, port=self.port, weight=self.weight), timeout=self.timeout)
                            if node_response.success == False:
                                pass
                            else:
                                self.nodes[response_addcon.node] = (node_stub, response_addcon.weight)
                        except Exception as e:
                            print(e)
                    node_num += 1
                    response_addcon = stub.addcon(store_pb2.AddConRequest(num=node_num), timeout=self.timeout)
                    
        except Exception as e:
            exit()

        super().__init__()

    def abort(self, request, context):
        if not self.was_slowed:
            try:
                if request.key != '':
                    del self.temp[request.key]
                    return store_pb2.AbortResponse(success=True)
                else:
                    return store_pb2.AbortResponse(success=False)
                
            except Exception as e:
                return store_pb2.AbortResponse(success=False)
        else:
            return store_pb2.AbortResponse(success=False)
        
    def addcon(self, request, context):
        if (len(list(self.nodes.keys()))) > request.num:
            node = list(self.nodes.keys())[request.num]
            return store_pb2.AddConResponse(node=node, weight=self.nodes[node][1])
        else:
            return store_pb2.AddConResponse(node='', weight=0)
        
    def commit(self, request, context):
        if not self.was_slowed:
            try:
                if request.key != '':
                    self.data[request.key] = self.temp[request.key]
                    try:
                        self.update_file(self.file, request.key, self.data[request.key])
                        return store_pb2.CommitResponse(success=True)
                    except Exception:
                        return store_pb2.CommitResponse(success=False)
                else:
                    return store_pb2.CommitResponse(success=False)
                
            except Exception as e:
                return store_pb2.CommitResponse(success=False)
        else:
            return store_pb2.CommitResponse(success=False)

    def get(self, request, context):
        if not self.was_slowed:
            try: 
                if request.key != '':
                    values = {}
                    values[self.data[request.key]] = self.weight
                    for stub in self.nodes.values():
                        try:
                            get_response = stub[0].privGet(request)
                        except Exception:
                            pass
                        if get_response.found:
                            if get_response.value in values:
                                values[get_response.value] += stub[1]
                            else:
                                values[get_response.value] = 0 + stub[1]
                        
                    maxKey = max(values, key=lambda k: values[k])
                    if values[maxKey] > self.min_weight_for_get:
                        return store_pb2.GetResponse(found=True, value=maxKey)
                    else:
                        return store_pb2.GetResponse(found=False)
                else:
                    return store_pb2.GetResponse(found=False)
                
            except Exception as e:
                return store_pb2.GetResponse(found=False)
        else:
            return store_pb2.GetResponse(found=False)

    def prepare(self, request, context):
        if not self.was_slowed:
            try:
                if request.key != '':
                    self.temp[request.key] = request.value
                    return store_pb2.PrepareResponse(success=True)
                else:
                    return store_pb2.PrepareResponse(success=False)
                
            except Exception as e:
                return store_pb2.PrepareResponse(success=False)
        else:
            return store_pb2.PrepareResponse(success=False)
        
    def privGet(self, request, context):
        if not self.was_slowed:
            if request.key in self.data:
                return store_pb2.GetResponse(found=True, value=self.data[request.key])
            else:
                return store_pb2.GetResponse(found=False)
        else:
            return store_pb2.GetResponse(found=False)
        
    def put(self, request, context):
        if not self.was_slowed:
            try:
                key = request.key
                value = request.value
                
                prepare_responses = self.weight
                commit_responses = self.weight
                commit_nodes = []
                abort = False

                self.temp[key] = value

                for adress, stub in self.nodes.items():
                    try:
                        prepare_request = store_pb2.PrepareRequest(key=key, value=value)
                        prepare_response = stub[0].prepare(prepare_request, timeout=self.timeout)
                        if prepare_response.success:
                            prepare_responses += stub[1]
                    except Exception:
                        pass
                if prepare_responses >= self.min_weight_for_commit:
                    commit_request = store_pb2.CommitRequest(key=key)
                    for stub in self.nodes.values():
                        try:
                            commit_response = stub[0].commit(commit_request, timeout=self.timeout)
                            if commit_response.success:
                                commit_responses += stub[1]
                                commit_nodes.append(stub[0])
                        except Exception:
                            pass
                    if commit_responses >= self.min_weight_for_commit:
                        self.data[key] = value
                        self.update_file(self.file, key, value)
                    else:
                        abort = True
                else:
                    abort = True

                if abort:
                    abort_request = store_pb2.AbortRequest(key=key)
                    for stub in self.nodes.values():
                        stub[0].abort(abort_request, timeout=self.timeout)
                    return store_pb2.PutResponse(success=False)
                else:
                    return store_pb2.PutResponse(success=True)
                
            except Exception as e:
                return store_pb2.PutResponse(success=False)
        else: 
            return store_pb2.PutResponse(success=False)
        
    def read_file(self, file):
        try:
            with open(file, 'r') as f:
                for line in f:
                    try:
                        key, value = line.strip().split(': ')
                        self.data[key] = value
                    except ValueError:
                        continue
        except FileNotFoundError:
            pass

    def register(self, request, context):
        try:
            if f"{request.ip}:{request.port}" not in self.nodes:
                channel = grpc.insecure_channel(f"{request.ip}:{request.port}")
                self.nodes[f"{request.ip}:{request.port}"] = (store_pb2_grpc.KeyValueStoreStub(channel), request.weight)
            
                self.min_weight_for_commit = int(sum(value[1] for value in self.nodes.values()) * 0.75)
                self.min_weight_for_get = int(sum(value[1] for value in self.nodes.values()) * 0.5)

            return store_pb2.RegisterResponse(success=True)
        except Exception as e:
            return store_pb2.RegisterResponse(success=False)
        
    def restore(self, request, context):
        try:
            if self.was_slowed:
                self.delay = 0
                self.was_slowed = False
                return store_pb2.RestoreResponse(success=True)
            else:
                return store_pb2.RestoreResponse(success=True)
        except Exception as e:
            return store_pb2.RestoreResponse(success=False)

    def slowDown(self, request, context):
        try:
            self.delay = request.seconds
            self.was_slowed = True
            while self.delay > 0:
                time.sleep(1)
                self.delay -= 1

            self.was_slowed = False
            return store_pb2.SlowDownResponse(success=True)
        except Exception as e:
                return store_pb2.SlowDownResponse(success=False)

    def unregister(self, request, context):
        try:
            del self.nodes[f"{request.ip}:{request.port}"]

        except Exception as e:
            return store_pb2.RegisterResponse(success=False)

    def update_file(self, file, key, value):
        try:
            with open(file, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            with open(file, 'w') as f:
                f.write(f"{key}: {value}\n")
        else:
            found_key = False
            for i, line in enumerate(lines):
                if key in line:
                    lines[i] = f"{key}: {value}\n"
                    found_key = True
                    break
            
            if not found_key:
                lines.append(f"{key}: {value}\n")
            
            with open(file, 'w') as f:
                f.writelines(lines)
    
def serve(ip, port, master, weight, master_weight):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(NodeKeyValueStoreServicer(ip, port, master, weight, master_weight), server)
    server.add_insecure_port(f'{ip}:{port}')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve("localhost", 32771, "localhost:32770", 1, 1)
