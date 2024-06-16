import os
import sys

# Agrega al path de Python la carpeta 'proto' que está en el mismo directorio que este script
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/proto')

# Importaciones necesarias para el cliente gRPC y los mensajes generados
import store_pb2 as store_pb2
import store_pb2_grpc as store_pb2_grpc
import yaml
import grpc

# Función para realizar una solicitud GET al servidor gRPC
def get(key):
    request = store_pb2.GetRequest(key=key)  # Crea una solicitud de tipo GetRequest con la clave proporcionada
    response = stub.get(request)  # Realiza la solicitud al servidor gRPC a través del stub
    return response.value, response.found  # Devuelve el valor y si se encontró o no

# Función para realizar una solicitud PUT al servidor gRPC
def put(key, value):
    request = store_pb2.PutRequest(key=key, value=value)  # Crea una solicitud de tipo PutRequest con la clave y valor proporcionados
    response = stub.put(request)  # Realiza la solicitud al servidor gRPC a través del stub
    return response.success  # Devuelve True si la operación fue exitosa, False si falló

# Función para enviar una solicitud RESTORE al servidor gRPC
def restore():
    request = store_pb2.RestoreRequest()  # Crea una solicitud de tipo RestoreRequest (sin parámetros)
    response = stub.restore(request)  # Realiza la solicitud al servidor gRPC a través del stub
    return response.success  # Devuelve True si la operación fue exitosa, False si falló

# Función para enviar una solicitud SLOW al servidor gRPC
def slow_down(seconds):
    request = store_pb2.SlowDownRequest(seconds=seconds)  # Crea una solicitud de tipo SlowDownRequest con los segundos proporcionados
    response = stub.slowDown(request)  # Realiza la solicitud al servidor gRPC a través del stub
    return response.success  # Devuelve True si la operación fue exitosa, False si falló

# Abre y carga el archivo 'centralized_config.yaml' que contiene la configuración del sistema centralizado
with open('centralized_config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Obtiene la IP y puerto del servidor maestro desde la configuración cargada
ip = config["master"]["ip"]
port = config["master"]["port"]

# Establece un canal gRPC inseguro hacia el servidor maestro usando la IP y puerto obtenidos
channel = grpc.insecure_channel(f'{ip}:{port}')

# Crea un stub para interactuar con el servicio KeyValueStore del servidor maestro
stub = store_pb2_grpc.KeyValueStoreStub(channel)

# Ciclo principal del cliente de línea de comandos
while True:
    print("COMMANDS:")
    print("PUT key value")
    print("GET key")
    print("SLOW seconds")
    print("RESTORE")
    print("EXIT")
    
    # Lee la entrada del usuario para determinar la acción a realizar
    command = input()
    os.system('cls')  # Limpia la pantalla para mejorar la legibilidad

    command_split = command.split(" ")  # Divide la entrada en palabras separadas por espacios
    command_split[0] = command_split[0].upper()  # Convierte el primer elemento a mayúsculas para aceptar comandos en mayúsculas o minúsculas

    # Manejo de los comandos
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

    print()  # Imprime una línea en blanco para mejor separación visual
    print("------------------------------------------------")  # Línea separadora visual