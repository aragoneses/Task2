import sys
import os

# Añadir el directorio del archivo proto al path para importar los módulos generados
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../proto')

# Importar los módulos generados por protobuf
import store_pb2 as store_pb2
import store_pb2_grpc as store_pb2_grpc

# Importar módulos necesarios de gRPC
from concurrent import futures
import time
import grpc

# Clase que implementa el servicio gRPC KeyValueStoreServicer para el nodo maestro
class MasterKeyValueStoreServicer(store_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        self.slave = {}     # Diccionario para almacenar los stubs de los nodos esclavos
        self.delay = 0      # Tiempo de retardo simulado para ralentizar operaciones
        self.was_slowed = False  # Indicador de si se ha aplicado retardo
        super().__init__()

    # Método para obtener un valor asociado a una clave
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

    # Método para almacenar un nuevo valor asociado a una clave
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

                # Preparar operación en todos los nodos esclavos
                for adress, stub in self.slave.items():
                    prepare_request = store_pb2.PrepareRequest(key=key, value=value)
                    prepare_response = stub.prepare(prepare_request)
                    prepare_responses.append(prepare_response)

                # Si todas las preparaciones tienen éxito, realizar el commit en todos los nodos esclavos
                if all(response.success for response in prepare_responses):
                    commit_request = store_pb2.CommitRequest(key=key)
                    for stub in self.slave.values():
                        commit_responses.append(stub.commit(commit_request))
                else: 
                    abort = True
                    
                # Si hubo algún fallo en las preparaciones, abortar la operación en todos los nodos
                if abort and not all(response.success for response in prepare_responses):
                    abort_request = store_pb2.AbortRequest(key=key)
                    for stub in self.slave.values():
                        stub.abort(abort_request)

                # Devolver éxito o fracaso de la operación
                if abort:
                    return store_pb2.PutResponse(success=False)
                else:
                    return store_pb2.PutResponse(success=True)
                
            except Exception as e:
                return store_pb2.PutResponse(success=False)
        else:
            return store_pb2.PutResponse(success=False)

    # Método para registrar un nodo esclavo en el nodo maestro
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
        
    # Método para restaurar el estado después de ralentizar las operaciones
    def restore(self, request, context):
        if self.was_slowed:
            self.delay = 0
            self.was_slowed = False
            return store_pb2.RestoreResponse(success=True)
        else:
            return store_pb2.RestoreResponse(success=True)

    # Método para simular ralentización de operaciones
    def slowDown(self, request, context):
        self.delay = request.seconds
        self.was_slowed = True
        while self.delay > 0:
            time.sleep(1)
            self.delay -= 1
        return store_pb2.SlowDownResponse(success=True)

    # Método para desregistrar un nodo esclavo del nodo maestro
    def unregister(self, request, context):
        if not self.was_slowed:
            try:
                del self.slave[f"{request.ip}:{request.port}"]
                return store_pb2.RegisterResponse(success=True)
            except Exception as e:
                return store_pb2.RegisterResponse(success=False)
        else: 
            return store_pb2.RegisterResponse(success=False)

# Función para iniciar el servidor gRPC del nodo maestro
def serve(ip, port):
    print(f"[MASTER] Serve at {ip}:{port}")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(MasterKeyValueStoreServicer(), server)
    server.add_insecure_port(f'{ip}:{port}') 
    server.start()
    server.wait_for_termination()

# Punto de entrada del programa
if __name__ == '__main__':
    serve("localhost", 50051)
