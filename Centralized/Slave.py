import sys
import os
import threading
import yaml

# Añadir el directorio del archivo proto al path para importar los módulos generados
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../proto')

# Importar módulos necesarios de gRPC y protobuf
import grpc
import store_pb2 as store_pb2
import store_pb2_grpc as store_pb2_grpc
from concurrent import futures
import time

# Clase que implementa el servicio gRPC KeyValueStoreServicer para el nodo esclavo
class SlaveKeyValueStoreServicer(store_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, ip, port, master):
        self.data = {}           # Diccionario para almacenar los datos clave-valor locales
        self.temp = {}           # Diccionario temporal para transacciones pendientes
        self.ip = ip             # Dirección IP del nodo esclavo
        self.port = port         # Puerto del nodo esclavo
        self.was_slowed = False  # Indicador de si se ha aplicado retardo
        self.delay = 0           # Tiempo de retardo simulado para ralentizar operaciones
        self.file = f"storage.json"  # Nombre del archivo de almacenamiento local
        
        self.read_file(self.file)  # Leer datos almacenados localmente al iniciar

        try:
            # Conectar con el nodo maestro utilizando gRPC
            channel = grpc.insecure_channel(f'{master}')
            self.stub = store_pb2_grpc.KeyValueStoreStub(channel)
            
            # Registrar este nodo esclavo en el nodo maestro
            response = self.stub.register(store_pb2.RegisterRequest(ip=self.ip, port=self.port))
            
            # Si no se pudo registrar correctamente, desregistrar y salir
            if not response.success:
                self.stub.unregister(store_pb2.UnregisterRequest(ip=self.ip, port=self.port))
                exit()
                
        except Exception as e:
            exit()
        
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
        
    # Método para confirmar una transacción pendiente
    def commit(self, request, context):
        if not self.was_slowed:
            try:
                if request.key != '':
                    self.data[request.key] = self.temp[request.key]
                    self.update_file(self.file, request.key, self.data[request.key])
                    return store_pb2.CommitResponse(success=True)
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
                    if request.key in self.data:
                        return store_pb2.GetResponse(found=True, value=self.data[request.key])
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

    # Método para leer datos desde un archivo local al iniciar
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

    # Método para restaurar el estado después de ralentizar las operaciones
    def restore(self, request, context):
        try:
            if self.was_slowed:
                self.delay = 0
                return store_pb2.RestoreResponse(success=True)
            else:
                return store_pb2.RestoreResponse(success=True)
        except Exception as e:
            return store_pb2.RestoreResponse(success=False)
        
    # Método para simular ralentización de operaciones
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

    # Método para actualizar el archivo local de almacenamiento
    def update_file(self, file, key, value):
        try:
            with open(file, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            with open(file, 'w') as f:
                f.write(f"{key}: {value}\n")
        else:
            foundKey = False
            for i, line in enumerate(lines):
                if key in line:
                    lines[i] = f"{key}: {value}\n"
                    foundKey = True
                    break
            
            if not foundKey:
                lines.append(f"{key}: {value}\n")
            
            with open(file, 'w') as f:
                f.writelines(lines)

# Función para iniciar el servidor gRPC del nodo esclavo
def serve(ip, port, master):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(SlaveKeyValueStoreServicer(ip, port, master), server)
    server.add_insecure_port(f'{ip}:{port}')
    server.start()
    server.wait_for_termination()

# Punto de entrada del programa
if __name__ == '__main__':
    # Cargar la configuración desde el archivo YAML
    with open('decentralized_config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # Obtener la configuración de los nodos esclavos
    slave_configs = config["nodes"]

    # Obtener la dirección del nodo maestro y su peso (utilizado en un contexto no implementado aquí)
    node0_host = f"{slave_configs[0]['ip']}:{slave_configs[0]['port']}"
    node0_weight = slave_configs[0]['weight']

    slaves = []

    # Iniciar un hilo para cada nodo esclavo en la configuración
    for slave_config in slave_configs:
        slave = threading.Thread(
            target=serve,
            args=(slave_config['ip'], slave_config['port'], node0_host)
        )
        slave.start()
        slaves.append(slave)
        time.sleep(1)  # Pequeña pausa entre el inicio de cada hilo para evitar conflictos de puerto

    # Esperar a que todos los hilos terminen
    for slave in slaves:
        slave.join()
