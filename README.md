# AvatarAirMaster

- Entrar a la carpeta del proyecto AVATARAIRMASTER 

`docker-compose up`

- Esperar a que todo levante e ir a la direccion `localhost:8080` donde se podra ver la ui de kafka 

- obtener los topicos en kafka :

    `docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092`

- para obtener el numero de mensajes en el topico :

    `sh messages_count.sh air_quality`

- Para reiniciar la infraestructura con eliminacion de todos los datos ejecutar el script : 

    `sh messages_count.sh`
