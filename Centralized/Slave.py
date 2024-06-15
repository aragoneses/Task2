import sys
import os
import threading
import yaml  # Importa el módulo yaml

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../proto')

import grpc
import store_pb2 as store_pb2
import store_pb2_grpc as store_pb2_grpc
from concurrent import futures
import random
import time

class SlaveKeyValueStoreServicer(store_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, ip, port, master):
        self.data = {}
        self.temp = {}
        self.ip = ip
        self.port = port
        self.was_slowed = False
        self.delay = 0
        self.archivo = f"storage.json"

        self.leer_archivo_txt(self.archivo)

        try:
            channel = grpc.insecure_channel(f'{master}')
            self.stub = store_pb2_grpc.KeyValueStoreStub(channel)
            response = self.stub.register(store_pb2.RegisterRequest(ip=self.ip, port=self.port))
            if response.success == False:
                # print(f"[SLAVE-{self.port}] Faliled to conect to MASTER, success :", response.success)
                self.stub.unregister(store_pb2.UnregisterRequest(ip=self.ip, port=self.port))
                exit()
            else:
                # print(f"[SLAVE-{self.port}] Registered at MASTER")
                pass
                
        except Exception as e:
            # print(f"[SLAVE-{self.port}] Faliled to conect to MASTER")
            # print(f"[SLAVE-{self.port}]", e)
            exit()
        super().__init__()

    def actualizar_archivo_txt(self, archivo, clave, valor):
        try:
            # Intentar abrir el archivo en modo lectura
            with open(archivo, 'r') as f:
                lineas = f.readlines()
        except FileNotFoundError:
            # Si el archivo no existe, crearlo y escribir la clave-valor
            with open(archivo, 'w') as f:
                f.write(f"{clave}: {valor}\n")
        else:
            # Buscar si la clave ya existe
            clave_encontrada = False
            for i, linea in enumerate(lineas):
                if clave in linea:
                    # Si la clave existe, actualizar el valor
                    lineas[i] = f"{clave}: {valor}\n"
                    clave_encontrada = True
                    break
            
            # Si la clave no existe, añadir al final del archivo
            if not clave_encontrada:
                lineas.append(f"{clave}: {valor}\n")
            
            # Escribir todas las líneas de nuevo en el archivo
            with open(archivo, 'w') as f:
                f.writelines(lineas)
    
    def leer_archivo_txt(self, archivo):
        try:
            with open(archivo, 'r') as f:
                for linea in f:
                    # Dividir la línea en clave y valor
                    try:
                        clave, valor = linea.strip().split(': ')
                        self.data[clave] = valor
                    except ValueError:
                        # Si la línea no tiene el formato esperado, ignorarla
                        continue
        except FileNotFoundError:
            # Si el archivo no existe, simplemente ignorarlo
            pass



    def prepare(self, request, context):
        if not self.was_slowed:
            try:
                if request.key != '':
                    # print(f"[SLAVE-{self.port}] Preparing to commit '{request.key}' : '{request.value}'")
                    self.temp[request.key] = request.value
                    return store_pb2.PrepareResponse(success=True)
                else:
                    # print(f"[SLAVE-{self.port}] No key provided")
                    return store_pb2.PrepareResponse(success=False)
                
            except Exception as e:
                # print(f"[SLAVE-{self.port}] Failed to prepare commit '{request.key}' : '{request.value}'")
                # print(f"[SLAVE-{self.port}]", e)
                return store_pb2.PrepareResponse(success=False)
        else:
            # print(f"[SLAVE-{self.port}] Slowed down, not preparing commit '{request.key}' : '{request.value}'")
            return store_pb2.PrepareResponse(success=False)
        
    def commit(self, request, context):
        if not self.was_slowed:
            try:
                if request.key != '':
                    # print(f"[SLAVE-{self.port}] Commiting key : {request.key}")
                    self.data[request.key] = self.temp[request.key]
                    self.actualizar_archivo_txt(self.archivo, request.key, self.data[request.key])
                    return store_pb2.CommitResponse(success=True)
                else:
                    # print(f"[SLAVE-{self.port}] Can't commit, no key provided")
                    return store_pb2.CommitResponse(success=False)
                
            except Exception as e:
                # print(f"[SLAVE-{self.port}] Failed to commit key : {request.key}")
                # print(f"[SLAVE-{self.port}]", e)
                return store_pb2.CommitResponse(success=False)
        else:
            # print(f"[SLAVE-{self.port}] Can't commit, was slowed down")
            return store_pb2.CommitResponse(success=False)

    def abort(self, request, context):
        if not self.was_slowed:
            try:
                if request.key != '':
                    # print(f"[SLAVE-{self.port}] Aborting key : {request.key}")
                    del self.temp[request.key]
                    return store_pb2.AbortResponse(success=True)
                else:
                    # print(f"[SLAVE-{self.port}] Can't abort, no key provided")
                    return store_pb2.AbortResponse(success=False)
                
            except Exception as e:
                # print(f"[SLAVE-{self.port}] Failed to abort key : {request.key}")
                # print(f"[SLAVE-{self.port}]", e)
                return store_pb2.AbortResponse(success=False)
        else:
            # print(f"[SLAVE-{self.port}] Can't abort, was slowed down")
            return store_pb2.AbortResponse(success=False)

    def get(self, request, context):
        if not self.was_slowed:
            try: 
                if request.key != '':
                    if request.key in self.data:
                        # print(f"[SLAVE-{self.port}] Getting key : {request.key}")
                        return store_pb2.GetResponse(found=True, value=self.data[request.key])
                    else:
                        # print(f"[SLAVE-{self.port}] Key : {request.key} not found")
                        return store_pb2.GetResponse(found=False)
                else:
                    # print(f"[SLAVE-{self.port}] Cant get, no key provided")
                    return store_pb2.GetResponse(found=False)
            
            except Exception as e:
                # print(f"[SLAVE-{self.port}] Failed to get key : '{request.key}'")
                # print(f"[SLAVE-{self.port}]", e)
                return store_pb2.GetResponse(found=False)
        else:
            # print(f"[SLAVE-{self.port}] Can't get, server is slow")
            return store_pb2.GetResponse(found=False)

    def slowDown(self, request, context):
        try:
            # print(f"[SLAVE-{self.port}] Slowing down for {request.seconds} seconds")
            self.delay = request.seconds
            self.was_slowed = True
            while self.delay > 0:
                time.sleep(1)
                self.delay -= 1
            self.was_slowed = False
            # print(f"[SLAVE-{self.port}] SlowDown terminated")
            return store_pb2.SlowDownResponse(success=True)
        
        except Exception as e:
            # print(f"[SLAVE-{self.port}] Failed to slow down")
            # print(f"[SLAVE-{self.port}]", e)
            return store_pb2.SlowDownResponse(success=False)

    def restore(self, request, context):
        try:
            if self.was_slowed:
                # print(f"[SLAVE-{self.port}] Restoring data")
                self.delay = 0
                return store_pb2.RestoreResponse(success=True)
            else:
                # print(f"[SLAVE-{self.port}] Was not slowed")
                return store_pb2.RestoreResponse(success=True)
        except Exception as e:
            # print(f"[SLAVE-{self.port}] Failed to restore data")
            # print(f"[SLAVE-{self.port}]", e)
            return store_pb2.RestoreResponse(success=False)

def serve(ip, port, master):
    # print(f"[SLAVE-{port}] Serve at {ip}:{port}")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    store_pb2_grpc.add_KeyValueStoreServicer_to_server(SlaveKeyValueStoreServicer(ip, port, master), server)
    server.add_insecure_port(f'{ip}:{port}')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    # Cargar la configuración desde el archivo YAML
    with open('decentralized_config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # Obtener la configuración de los nodos
    slave_configs = config["nodes"]

    # Configuración del primer nodo (node0)
    node0_host = f"{slave_configs[0]['ip']}:{slave_configs[0]['port']}"
    node0_weight = slave_configs[0]['weight']

    # Lista para almacenar los hilos de los nodos esclavos
    slaves = []

    # Iniciar hilos para cada nodo esclavo en la configuración
    for slave_config in slave_configs:
        slave = threading.Thread(
            target=serve,
            args=(slave_config['ip'], slave_config['port'], node0_host)
        )
        slave.start()
        slaves.append(slave)
        time.sleep(1)  # Esperar un segundo antes de iniciar el siguiente hilo

    # Esperar a que todos los hilos terminen
    for slave in slaves:
        slave.join()

