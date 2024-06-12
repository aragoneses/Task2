import grpc
import time
from concurrent import futures
from proto import store_pb2_grpc as store_pb2_grpc
from proto import store_pb2 as store_pb2
from decentralized_nodes.destore_servicer import KeyValueStoreServicer

# Crear un servidor gRPC
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

# Configurar puerto y amigo
myport = 32791
port1 = 32792

# Iniciar el servidor gRPC
print('Iniciando servidor. Escuchando en el puerto 32780.')
server.add_insecure_port(f'0.0.0.0:{myport}')
server.start()

# Agregar el KeyValueStoreServicer al servidor
store_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStoreServicer(32780, 32781, 32782, 1), server)

# Conectar con el amigo y obtener sus puertos
friend_channel = grpc.insecure_channel(f'localhost:{port1}')
friend_stub = store_pb2_grpc.KeyValueStoreStub(friend_channel)

# Conectar con mi propio servidor
my_channel = grpc.insecure_channel(f'localhost:{myport}')
my_stub = store_pb2_grpc.KeyValueStoreStub(my_channel)

# Esperar un momento para permitir que el servidor se establezca
time.sleep(0.5)

# Obtener los puertos de los amigos y agregarlos a mi lista de puertos conocidos
friends_ports = friend_stub.discover(store_pb2.DiscRequest(port=port1))
print("Puertos de los amigos: " + friends_ports.ports)
my_stub.addPorts(store_pb2.portRequest(ports=friends_ports.ports))

# Como server.start() no bloquea, se agrega un bucle de sleep para mantenerlo vivo
try:
    while True:
        time.sleep(86400)  # Dormir por un día
except KeyboardInterrupt:
    server.stop(0)  # Detener el servidor cuando se presiona Ctrl+C
