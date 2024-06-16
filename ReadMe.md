## Proyecto de Almacenamiento Distribuido de Clave-Valor con gRPC

## Descripción del Proyecto

Este proyecto implementa sistemas centralizados y descentralizados de almacenamiento de clave-valor en Python utilizando gRPC. En el sistema descentralizado, los nodos se comunican entre sí para registrar, almacenar y recuperar datos, con capacidades de preparación y confirmación. El sistema centralizado, a través de un maestro, coordina las operaciones entre esclavos para garantizar la consistencia de los datos. Se incluyen funciones para registrar, restaurar, ralentizar y eliminar nodos, así como para ejecutar pruebas automatizadas. Se utilizan hilos y manejo de excepciones para garantizar la robustez del sistema. El proyecto se estructura en clases y métodos que siguen las convenciones de estilo de Python y se prueban con scripts de evaluación para validar su funcionamiento.

## Diseño del Sistema
## Nodos Maestros y Esclavos
El sistema se basa en la arquitectura cliente-servidor utilizando gRPC para la comunicación entre los diferentes componentes.

Nodo Maestro (Master Node)
El nodo maestro despliega un servicio gRPC que permite realizar operaciones como:

Obtener (get)
Almacenar (put)
Registrar nodos esclavos
Restaurar estados
Ralentizar operaciones
Desregistrar nodos
Este nodo actúa como coordinador central para la gestión de transacciones distribuidas y la interacción con los nodos esclavos.

## Nodos Descentralizados (Node)
Los nodos descentralizados implementan un servicio gRPC para el almacenamiento de clave-valor en un entorno descentralizado. Estos nodos pueden ejecutar operaciones como:

Obtener valores
Almacenar nuevos valores
Registrarse en el nodo maestro
Restaurar estados
Ralentizar operaciones
Desregistrarse
Además, son responsables de la gestión local de datos y la comunicación con otros nodos para garantizar la consistencia de la información.

## Instalación

Primero tienes que instalar las dependencias mediante:
pip install -r requirements.txt

Luego, puedes ejecutar los tests, haciendo python eval/centralized_system_tests y python eval/decentralized_system_tests