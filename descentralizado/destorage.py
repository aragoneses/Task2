import json
import grpc
import os
import storage_pb2
import storage_pb2_grpc
from concurrent import futures

# Archivo de almacenamiento
STORAGE_FILE = 'destore.json'

class StorageServicer(storage_pb2_grpc.StorageServiceServicer):
    def __init__(self):
        self.keys_set = set()  # Conjunto de claves
        self.db = dict()  # Base de datos como diccionario
        self.load_storage()  # Cargar datos almacenados

    def load_storage(self):
        # Cargar datos almacenados desde el archivo si existe
        if os.path.exists(STORAGE_FILE):
            with open(STORAGE_FILE, 'r') as f:
                data = json.load(f)
                self.db = data
                self.keys_set = set(data.keys())
                print("Almacenamiento cargado correctamente.")
        else:
            print("No se encontró archivo de almacenamiento. Iniciando con una base de datos vacía.")

    def save_storage(self):
        # Guardar datos almacenados en el archivo
        with open(STORAGE_FILE, 'w') as f:
            json.dump(self.db, f)

    def SaveValue(self, request, context):
        # Guardar un valor en la base de datos
        self.keys_set.add(request.key)
        self.db[request.key] = request.data
        self.save_storage()  # Guardar cambios en el archivo
        return storage_pb2.Empty()

    def GetValue(self, request, context):
        # Obtener un valor dado una clave
        data = self.db.get(request.key, "")
        return storage_pb2.Value(key=request.key, data=data)

    def GetAllValues(self, request, context):
        # Obtener todos los valores almacenados
        values = [storage_pb2.Value(key=k, data=v) for k, v in self.db.items()]
        return storage_pb2.ValueList(values=values)

def serve():
    # Iniciar el servidor gRPC
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    storage_pb2_grpc.add_StorageServiceServicer_to_server(StorageServicer(), server)
    server.add_insecure_port('[::]:50052')  # Puerto sin cifrado
    server.start()
    print("Servidor iniciado. Escuchando en el puerto 50052...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
