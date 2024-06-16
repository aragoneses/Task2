import sys
import os

# Añade al path de Python la carpeta 'Centralized' que está en el mismo directorio que este script
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/Centralized')

# Importa la función 'serve' del módulo 'Master' y 'Slave' de la carpeta 'Centralized'
from Master import serve as serve_master
from Slave import serve as serve_slave

import yaml
import threading

if __name__ == '__main__':
    # Abre y carga el archivo 'centralized_config.yaml' que contiene la configuración del sistema centralizado
    with open('centralized_config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # Inicia un hilo para el servidor maestro usando la función 'serve_master' con la IP y puerto especificados en la configuración
    master_thread = threading.Thread(target=serve_master, args=(config["master"]["ip"], config["master"]["port"]))
    master_thread.start()

    # Configuración de los esclavos
    slave_configs = config["slaves"]
    slaves = []
    for slave_config in slave_configs:
        # Inicia un hilo para cada esclavo usando la función 'serve_slave' con la IP, puerto y la dirección del maestro
        slave = threading.Thread(target=serve_slave, args=(slave_config['ip'], slave_config['port'], f'{config["master"]["ip"]}:{config["master"]["port"]}'))
        slave.start()
        slaves.append(slave)

    # Espera a que el hilo del servidor maestro termine su ejecución
    master_thread.join()

    # Espera a que todos los hilos de los esclavos terminen su ejecución
    for slave in slaves:
        slave.join()
