import sys
import os

# Añadir el directorio del archivo proto al path para importar los módulos generados
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../proto')

import grpc
import store_pb2 as store_pb2
import store_pb2_grpc as store_pb2_grpc
from concurrent import futures
import time

# Clase que implementa el servicio gRPC KeyValueStoreServicer para el nodo
class NodeKeyValueStoreServicer(store_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, ip, port, node0_host, weight, node0_weight):
        self.data = {}                   # Diccionario para almacenar datos clave-valor
        self.temp = {}                   # Diccionario temporal para transacciones pendientes
        self.nodes = {}                  # Diccionario para almacenar nodos conectados
        self.ip = ip                     # Dirección IP del nodo actual
        self.port = port                 # Puerto del nodo actual
        self.was_slowed = False          # Indicador de si se ha aplicado retardo
        self.weight = weight             # Peso del nodo actual
        self.min_weight_for_commit = 0   # Peso mínimo requerido para confirmar transacciones
        self.min_weight_for_get = 0      # Peso mínimo requerido para obtener valores
        self.timeout = 0.1               # Tiempo de espera para las operaciones gRPC
        self.delay = 0                   # Retraso simulado para ralentizar operaciones
        self.file = f"destorage.json"    # Nombre del archivo de almacenamiento local

        self.read_file(self.file)  # Leer datos almacenados localmente al iniciar

        try:
            # Registro del nodo actual en el nodo maestro (node0_host)
            if f"{ip}:{port}" != node0_host:
                channel = grpc.insecure_channel(f'{node0_host}')
                stub = store_pb2_grpc.KeyValueStoreStub(channel)
                
                # Intentar registrarse con el nodo maestro
                response = stub.register(store_pb2.RegisterRequest(ip=self.ip, port=self.port, weight=self.weight), timeout=self.timeout)
                
                # Almacenar el stub del nodo maestro y su peso
                self.nodes[node0_host] = (stub, node0_weight)
                
                if not response.success:
                    exit()  # Salir si no se pudo registrar correctamente
                
                # Proceso para añadir conexiones con otros nodos
                node_num = 0
                response_addcon = stub.addcon(store_pb2.AddConRequest(num=node_num), timeout=self.timeout)
                
                while response_addcon.node != '':
                    if response_addcon.node != f'{self.ip}:{self.port}':
                        try:
                            node_channel = grpc.insecure_channel(f'{response_addcon.node}')
                            node_stub = store_pb2_grpc.KeyValueStoreStub(node_channel)
                            
                            # Intentar registrarse con otro nodo
                            node_response = node_stub.register(store_pb2.RegisterRequest(ip=self.ip, port=self.port, weight=self.weight), timeout=self.timeout)
                            
                            if node_response.success:
                                self.nodes[response_addcon.node] = (node_stub, response_addcon.weight)
                            
                        except Exception as e:
                            print(e)  # Manejar errores de conexión con nodos
                        
                    node_num += 1
                    response_addcon = stub.addcon(store_pb2.AddConRequest(num=node_num), timeout=self.timeout)
                    
        except Exception as e:
            exit()  # Salir en caso de cualquier excepción durante la inicialización

        super().__init__()

    # Método para abortar una transacción pendiente
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

    # Método para agregar una conexión con un nodo especificado
    def addcon(self, request, context):
        if (len(list(self.nodes.keys()))) > request.num:
            node = list(self.nodes.keys())[request.num]
            return store_pb2.AddConResponse(node=node, weight=self.nodes[node][1])
        else:
            return store_pb2.AddConResponse(node='', weight=0)

    # Método para confirmar una transacción pendiente
    def commit(self, request, context):
        if not self.was_slowed:
            try:
                if request.key != '':
                    self.data[request.key] = self.temp[request.key]
                    
                    try:
                        # Actualizar el archivo de almacenamiento local
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

    # Método para obtener el valor asociado a una clave
    def get(self, request, context):
        if not self.was_slowed:
            try: 
                if request.key != '':
                    values = {}
                    values[self.data[request.key]] = self.weight
                    
                    # Consultar otros nodos conectados para obtener sus valores asociados a la clave
                    for stub in self.nodes.values():
                        try:
                            get_response = stub[0].privGet(request)
                            
                            if get_response.found:
                                if get_response.value in values:
                                    values[get_response.value] += stub[1]
                                else:
                                    values[get_response.value] = 0 + stub[1]
                                
                        except Exception:
                            pass  # Ignorar errores de conexión con nodos
                    
                    # Determinar el valor con el peso máximo y comprobar si supera el umbral mínimo
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

    # Método para preparar una transacción pendiente
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

    # Método para obtener un valor específico de manera privada
    def privGet(self, request, context):
        if not self.was_slowed:
            if request.key in self.data:
                return store_pb2.GetResponse(found=True, value=self.data[request.key])
            else:
                return store_pb2.GetResponse(found=False)
        
        else:
            return store_pb2.GetResponse(found=False)

    # Método para almacenar un valor asociado a una clave
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

                # Intentar preparar la transacción en todos los nodos conectados
                for address, stub in self.nodes.items():
                    try:
                        prepare_request = store_pb2.PrepareRequest(key=key, value=value)
                        prepare_response = stub[0].prepare(prepare_request, timeout=self.timeout)
                        
                        if prepare_response.success:
                            prepare_responses += stub[1]
                    
                    except Exception:
                        pass  # Ignorar errores de conexión con nodos

                # Comprobar si se supera el umbral mínimo para confirmar la transacción
                if prepare_responses >= self.min_weight_for_commit:
                    commit_request = store_pb2.CommitRequest(key=key)

                    for stub in self.nodes.values():
                        try:
                            commit_response = stub[0].commit(commit_request, timeout=self.timeout)
                            
                            if commit_response.success:
                                commit_responses += stub[1]
                                commit_nodes.append(stub[0])
                        
                        except Exception:
                            pass  # Ignorar errores de conexión con nodos

                    # Confirmar la transacción si se supera el umbral mínimo
                    if commit_responses >= self.min_weight_for_commit:
                        self.data[key] = value
                        self.update_file(self.file, key, value)
                    
                    else:
                        abort = True

                else:
                    abort = True

                # Abortar la transacción si no se puede confirmar correctamente
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

    # Método para leer datos del archivo de almacenamiento local
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

    # Método para registrar un nodo en el sistema distribuido
    def register(self, request, context):
        try:
            # Verificar si el nodo no está registrado previamente
            if f"{request.ip}:{request.port}" not in self.nodes:
                channel = grpc.insecure_channel(f"{request.ip}:{request.port}")
                self.nodes[f"{request.ip}:{request.port}"] = (store_pb2_grpc.KeyValueStoreStub(channel), request.weight)
            
                # Actualizar el peso mínimo requerido para confirmar y obtener valores
                self.min_weight_for_commit = int(sum(value[1] for value in self.nodes.values()) * 0.75)
                self.min_weight_for_get = int(sum(value[1] for value in self.nodes.values()) * 0.5)

            return store_pb2.RegisterResponse(success=True)
        
        except Exception as e:
            return store_pb2.RegisterResponse(success=False)

    # Método para restaurar el nodo a su estado original después de un retardo
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

    # Método para aplicar un retardo simulado en las operaciones del nodo
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

    # Método para quitar el registro de un nodo del sistema distribuido
    def unregister(self, request, context):
        try:
            del self.nodes[f"{request.ip}:{request.port}"]

        except Exception as e:
            return store_pb2.RegisterResponse(success=False)

    # Método para actualizar el archivo de almacenamiento local con un nuevo par clave-valor
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

# Función para iniciar el servidor gRPC del nodo
def serve(ip, port, master, weight, master_weight):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Agregar el servicio NodeKeyValueStoreServicer al servidor gRPC
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(NodeKeyValueStoreServicer(ip, port, master, weight, master_weight), server)
    
    # Iniciar el servidor en la dirección IP y puerto especificados
    server.add_insecure_port(f'{ip}:{port}')
    server.start()
    server.wait_for_termination()

# Punto de entrada principal del programa
if __name__ == '__main__':
    # Ejemplo de inicio del servidor en localhost, puerto 32771, conectándose al nodo maestro en localhost:32770
    serve("localhost", 32771, "localhost:32770", 1, 1)

