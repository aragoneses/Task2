import sys
import os
import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/Decentralized')

from Node import serve as serve

import yaml
import threading

if __name__ == '__main__':
    with open('decentralized_config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    slave_configs = config["nodes"]
    slaves = []

    node0_host = f"{slave_configs[0]['ip']}:{slave_configs[0]['port']}"
    node0_weight = slave_configs[0]["weight"]

    for slave_config in slave_configs:
        slave = threading.Thread(target=serve, args=(slave_config['ip'], slave_config['port'], node0_host, slave_config['weight'], node0_weight))
        slave.start()
        slaves.append(slave)
        time.sleep(1)

    for slave in slaves:
        slave.join()