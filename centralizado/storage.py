import json
import os
import grpc
from concurrent import futures
import storage_pb2
import storage_pb2_grpc

STORAGE_FILE = 'store.json'

class StorageServicer(storage_pb2_grpc.StorageServiceServicer):
    def __init__(self):
        self.keys_set = set()
        self.db = dict()
        self.load_storage()

    def load_storage(self):
        if os.path.exists(STORAGE_FILE):
            with open(STORAGE_FILE, 'r') as f:
                data = json.load(f)
                self.db = data
                self.keys_set = set(data.keys())
                print("Storage loaded successfully.")
        else:
            print("No storage file found. Starting with an empty database.")

    def save_storage(self):
        with open(STORAGE_FILE, 'w') as f:
            json.dump(self.db, f)
            print("Storage saved successfully.")

    def SaveValue(self, request, context):
        self.keys_set.add(request.key)
        self.db[request.key] = request.data
        self.save_storage()
        print(f"Saved value: {request.key} - {request.data}")
        return storage_pb2.Empty()

    def GetValue(self, request, context):
        data = self.db.get(request.key, "")
        return storage_pb2.Value(key=request.key, data=data)

    def GetAllValues(self, request, context):
        values = [storage_pb2.Value(key=k, data=v) for k, v in self.db.items()]
        return storage_pb2.ValueList(values=values)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    storage_pb2_grpc.add_StorageServiceServicer_to_server(StorageServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started. Listening on port 50051...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
