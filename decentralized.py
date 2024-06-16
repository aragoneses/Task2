import sys
import os
import time

# Añade al path de Python la carpeta 'Decentralized' que está en el mismo directorio que este script
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/Decentralized')

# Importación del módulo Node desde el paquete Decentralized
from Node import serve as serve

# Importaciones adicionales necesarias
import yaml
import threading

if __name__ == '__main__':
    # Lee la configuración del archivo 'decentralized_config.yaml'
    with open('decentralized_config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # Obtiene la configuración de los nodos esclavos desde la configuración cargada
    slave_configs = config["nodes"]
    slaves = []  # Lista para almacenar los hilos (threads) de los nodos esclavos

    # Obtiene la dirección IP y puerto del nodo 0 desde la configuración
    node0_host = f"{slave_configs[0]['ip']}:{slave_configs[0]['port']}"
    node0_weight = slave_configs[0]["weight"]

    # Itera sobre cada configuración de nodo esclavo para iniciar un hilo (thread) por cada uno
    for slave_config in slave_configs:
        # Crea un hilo (thread) que llama a la función serve del módulo Node con los parámetros correspondientes
        slave = threading.Thread(target=serve, args=(slave_config['ip'], slave_config['port'], node0_host, slave_config['weight'], node0_weight))
        slave.start()  # Inicia el hilo
        slaves.append(slave)  # Agrega el hilo a la lista de hilos (threads)
        time.sleep(1)  # Espera 1 segundo antes de iniciar el próximo hilo (para evitar congestionar el inicio)

    # Espera a que todos los hilos (threads) de los nodos esclavos terminen su ejecución
    for slave in slaves:
        slave.join()
