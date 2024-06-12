import grpc, time,os,sys
from concurrent import futures
import store_pb2_grpc
import KeyValueStoreServicer


# create a gRPC server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

# use the generated function `add_InsultingServiceServicer_to_server`
# to add the defined class to the server
store_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStoreServicer(32791,32790,32792,), server)



print('Starting server. Listening on port 32791.')
server.add_insecure_port('0.0.0.0:32791')
server.start()

# since server.start() will not block,
# a sleep-loop is added to keep alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)