import os
import signal
import subprocess
import sys
import time

def signal_handler(sig, frame):
    # Terminar los subprocesos
    print("\033[92mTerminando todos los subprocesos...\033[0m")
    for process in processes:
        process.terminate()
    sys.exit(0)

# Registrar el manejador de señales para SIGTERM y SIGINT
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# Lista para almacenar los subprocesos
processes = []

try:
    # Ejecutar el servidor y agregar el proceso a la lista
    server_process = subprocess.Popen([sys.executable, 'centralizado/storage.py'])
    processes.append(server_process)

    # Ejecutar el primer script y agregar el proceso a la lista
    server_process = subprocess.Popen([sys.executable, 'centralizado/master.py'])
    processes.append(server_process)

    # Ejecutar el segundo script y agregar el proceso a la lista
    node0_process = subprocess.Popen([sys.executable, 'centralizado/node1.py'])
    processes.append(node0_process)

    # Ejecutar el tercer script y agregar el proceso a la lista
    node1_process = subprocess.Popen([sys.executable, 'centralizado/node2.py'])
    processes.append(node1_process)

    # Comprobar si el proceso principal sigue en ejecución
    while True:
        if os.getppid() == 1:  # Si el ID del proceso padre es 1 (init), se termina el proceso principal
            signal_handler(signal.SIGTERM, None)
        time.sleep(1)  # Esperar 1 segundo antes de volver a comprobar

except KeyboardInterrupt:
    # Manejar Ctrl+C para terminar los subprocesos
    signal_handler(signal.SIGINT, None)
