import os
import sys

# Añade al path de Python la carpeta 'proto' que está en el mismo directorio que este script
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/proto')

# Importaciones necesarias
import yaml
import grpc
import store_pb2 as store_pb2
import store_pb2_grpc as store_pb2_grpc

# Función para realizar una solicitud 'get' al nodo seleccionado
def get(key):
    request = store_pb2.GetRequest(key=key)
    response = stub[node].get(request)
    return response.value, response.found

# Función para realizar una solicitud 'put' al nodo seleccionado
def put(key, value):
    request = store_pb2.PutRequest(key=key, value=value)
    response = stub[node].put(request)
    return response.success

# Función para restaurar el nodo seleccionado
def restore():
    request = store_pb2.RestoreRequest()
    response = stub[node].restore(request)
    return response.success

# Función para ralentizar el nodo seleccionado
def slow_down(seconds):
    request = store_pb2.SlowDownRequest(seconds=seconds)
    response = stub[node].slowDown(request)
    return response.success

# Lee la configuración del archivo 'decentralized_config.yaml'
with open('decentralized_config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Obtiene la configuración de los nodos desde la configuración cargada
node_configs = config["nodes"]
slaves = []  # Lista para almacenar los stubs de los nodos
stub = []
total = 0

# Crea un stub para cada nodo configurado y los almacena en la lista 'stub'
for node_config in node_configs:
    stub.append(store_pb2_grpc.KeyValueStoreStub(grpc.insecure_channel(f'{node_config["ip"]}:{node_config["port"]}')))
    total += 1

while True:
    nocont = False
    while not nocont:
        try:
            # Solicita al usuario que ingrese el índice del nodo deseado
            node = int(input(f"Enter node ( 0 - {total - 1} )\n"))
            if node < 0 or node >= total:
                print("Invalid node")
            else:
                nocont = True
        except Exception:
            pass

    # Imprime las opciones de comandos disponibles
    print("COMMANDS:")
    print("PUT key value")
    print("GET key")
    print("SLOW seconds")
    print("RESTORE")
    print("EXIT")
    
    # Lee la entrada del usuario
    command = input()
    os.system('cls')  # Limpia la pantalla (para Windows)
    
    # Divide el comando en palabras y convierte el primer elemento a mayúsculas
    command_split = command.split(" ")
    command_split[0] = command_split[0].upper()
    
    # Procesamiento de los comandos ingresados por el usuario
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
    
    # Imprime líneas adicionales para mejorar la legibilidad en la consola
    print()
    print("------------------------------------------------")
    print()