# Description: This file contains the implementation of the KeyValueStoreServicer class, which is responsible for 
#handling the gRPC requests made by the client. The KeyValueStoreServicer class implements the KeyValueStoreServicer
#interface defined in the store_pb2_grpc module. 
import store_pb2, store_pb2_grpc, storage_pb2, storage_pb2_grpc
from servicer import store_service
import time,grpc

class KeyValueStoreServicer(store_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, myport, port1, port2):
        self.time = 0
        self.ports = []

        if (myport == 32770):
            #Compte
            self.isMaster = True
        else:
            self.isMaster = False
            master_channel = grpc.insecure_channel(f'localhost:32790')
            master_stub = store_pb2_grpc.KeyValueStoreStub(master_channel)
            success = master_stub.addSlave(store_pb2.slaveRequest(port=myport))

        #Iniciem canal amb la base de dades
        self.db_channel = grpc.insecure_channel(f'localhost:50051')
        self.db_stub = storage_pb2_grpc.StorageServiceStub(self.db_channel)

        values = self.db_stub.GetAllValues(storage_pb2.Empty())
        for value in values.values:
            print(f"Received value from server: {value.key} - {value.data}")
            store_service.doCommit(value.key, value.data)

        #self.data = {}
        #aqui anira el lock

    def put(self, putRequest, context):
        time.sleep(self.time)
        if not self.isMaster:
            return store_pb2.PutResponse(success=False)
        
        success = store_service.put(putRequest.key, putRequest.value)
        response = store_pb2.PutResponse()
        response.success = success
        #CanCommit
        for port in self.ports:
            channel = grpc.insecure_channel(f'localhost:{port}') 
            stub = store_pb2_grpc.KeyValueStoreStub(channel)
            response_commit = stub.canCommit(store_pb2.google_dot_protobuf_dot_empty__pb2.Empty())
        
        for port in self.ports:
            channel = grpc.insecure_channel(f'localhost:{port}') 
            stub = store_pb2_grpc.KeyValueStoreStub(channel)
            response_docommit = stub.doCommit(store_pb2.DoCommitRequest(key=putRequest.key, value=putRequest.value))
            print("Put request response received with key: ")

        value = storage_pb2.Value(key=putRequest.key, data=putRequest.value)
        self.db_stub.SaveValue(value)

        return response
         #comprovar si som master
        #si som master, agafar lock i two phase commit (nse ordre)

    def canCommit(self, empty, context):
        
        #time.sleep(self.time)
        return store_pb2.CanCommitResponse(success=True)
    
    def doCommit(self, doCommitRequest, context):
        store_service.doCommit(doCommitRequest.key, doCommitRequest.value)
        #time.sleep(self.time)
        response = store_pb2.google_dot_protobuf_dot_empty__pb2.Empty()
        return response

    def get(self, getRequest, context):
        #mirar si el lock esta agafat
        #si esta agafat, esperar poc temps
        #retornar valor
        value,found = store_service.get(getRequest.key)
        response = store_pb2.GetResponse()
        response.value = value
        response.found = found
        print("Get request response received with key: " + str(response.found) +response.value)
        time.sleep(self.time)
        return response

    def slowDown(self, slowDownRequest, context):
        try:
            self.time = slowDownRequest.seconds
            print("SlowDown request received with seconds: " + str(self.time))
        except (AttributeError, TypeError) as e:
            # Handle specific exceptions
            self.time = 0
            return store_pb2.SlowDownResponse(success=False, error_message=str(e))
        
        time.sleep(self.time)
        return store_pb2.SlowDownResponse(success=True)

    def restore(self, empty, context):
        time.sleep(self.time)
        self.time = 0
        return store_pb2.RestoreResponse(success=True)
    
    def addSlave(self, slaveRequest, context):
        port = store_service.addSlave(slaveRequest.port)
        if self.isMaster:
            self.ports.append(slaveRequest.port)
            print("Slave added...")
            print("Master ports:"+str(self.ports))
            return store_pb2.slaveResponse(success= True)
        else:
            return store_pb2.slaveResponse(success=False)

    