import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/Centralized')

from Master import serve as serve_master
from Slave import serve as serve_slave

import yaml
import threading

if __name__ == '__main__':
    with open('centralized_config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    master_thread = threading.Thread(target=serve_master, args=(config["master"]["ip"], config["master"]["port"]))
    master_thread.start()

    slave_configs = config["slaves"]
    slaves = []
    for slave_config in slave_configs:
        slave = threading.Thread(target=serve_slave, args=(slave_config['ip'], slave_config['port'], f'{config["master"]["ip"]}:{config["master"]["port"]}'))
        slave.start()
        slaves.append(slave)

    master_thread.join()

    for slave in slaves:
        slave.join()