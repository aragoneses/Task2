import time
import grpc
from proto import store_pb2, store_pb2_grpc, storage_pb2, storage_pb2_grpc
from decentralized_nodes.destore_service import dataStore

class KeyValueStoreServicer(store_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, myport, port1, port2, weight):
        self.time = 0
        self.ports = [port1, myport]  # Inicializando los puertos conocidos
        self.weight = weight
        self.READ_QUORUM = 2
        self.WRITE_QUORUM = 3
        
        # Iniciando conexión con la base de datos
        self.db_channel = grpc.insecure_channel('localhost:50052')
        self.db_stub = storage_pb2_grpc.StorageServiceStub(self.db_channel)

        # Recuperando valores de la base de datos
        values = self.db_stub.GetAllValues(storage_pb2.Empty())
        for value in values.values:
            print(f"Valor recibido del servidor: {value.key} - {value.data}")
            store_service.doCommit(value.key, value.data)
        
        print(f"elport: {myport} port1: {port1}")

    def put(self, putRequest, context):
        quorum = self.weight
        time.sleep(self.time)

        # Solicitando votos
        for port in self.ports:
            channel = grpc.insecure_channel(f'localhost:{port}') 
            stub = store_pb2_grpc.KeyValueStoreStub(channel)
            response_quorum = stub.askVote(store_pb2.AskRequest(key=putRequest.key))
            quorum += response_quorum.weight

            if quorum >= self.WRITE_QUORUM:
                success = True
            else:
                success = False

        if success:
            put_response = store_service.put(putRequest.key, putRequest.value)  
            if put_response:
                for port in self.ports:
                    channel = grpc.insecure_channel(f'localhost:{port}') 
                    stub = store_pb2_grpc.KeyValueStoreStub(channel)
                    response_docommit = stub.doCommit(store_pb2.DoCommitRequest(key=putRequest.key, value=putRequest.value))

        value = storage_pb2.Value(key=putRequest.key, data=putRequest.value)
        self.db_stub.SaveValue(value)
        response_put = store_pb2.PutResponse(success=success)
        return response_put

    def get(self, getRequest, context):
        value, found = store_service.get(getRequest.key)
        response = store_pb2.GetResponse()
        response.value = value
        response.found = found
        
        quorum = 0
        for port in self.ports:
            channel = grpc.insecure_channel(f'localhost:{port}') 
            stub = store_pb2_grpc.KeyValueStoreStub(channel)
            response_quorum = stub.askVote(store_pb2.AskRequest(key=getRequest.key))

            if response_quorum.value == value:
                quorum += response_quorum.weight

            if quorum >= self.READ_QUORUM:
                response.found = True
            else:
                response.found = False

        time.sleep(self.time)
        return response

    def slowDown(self, slowDownRequest, context):
        try:
            self.time = slowDownRequest.seconds
        except (AttributeError, TypeError) as e:
            self.time = 0
            return store_pb2.SlowDownResponse(success=False, error_message=str(e))

        return store_pb2.SlowDownResponse(success=True)

    def restore(self, empty, context):
        self.time = 0
        return store_pb2.RestoreResponse(success=True)

    def askVote(self, askRequest, context):
        value = store_service.askVote(askRequest.key)
        response = store_pb2.AskResponse(weight=self.weight, value=value)
        return response
    
    def doCommit(self, doCommitRequest, context):
        store_service.doCommit(doCommitRequest.key, doCommitRequest.value)
        response = store_pb2.google_dot_protobuf_dot_empty__pb2.Empty()
        return response
    
    def discover(self, discRequest, context):
        disc_answer = ",".join(str(port) for port in self.ports)
        response = store_pb2.DiscResponse(ports=disc_answer)
        return response

    def addPorts(self, portRequest, context):  
        fr_ports = portRequest.ports.split(",")
        for item in fr_ports:
            if item != "":
                try:
                    port = int(item)
                    if port not in self.ports:
                        self.ports.append(port)
                except ValueError:
                    print(f"Ignorando valor de puerto inválido: {item}")

        return store_pb2.google_dot_protobuf_dot_empty__pb2.Empty()
