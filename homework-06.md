# Домашнее задание 6

- [Домашнее задание 6](#домашнее-задание-6)
  - [Задание](#задание)
  - [refs](#refs)
  - [Решение](#решение)
    - [подготовка](#подготовка)
    - [запуск replica set конфигурации](#запуск-replica-set-конфигурации)
    - [запуск replica set данных №1](#запуск-replica-set-данных-1)
    - [запуск replica set данных №2](#запуск-replica-set-данных-2)
    - [запуск mongos и добавление шардов](#запуск-mongos-и-добавление-шардов)
    - [запуск percona-backup-mongodb](#запуск-percona-backup-mongodb)
    - [загрузка тестовых данных](#загрузка-тестовых-данных)
    - [создание бекапа](#создание-бекапа)
    - [удаление данных](#удаление-данных)
    - [восстановление данных из бекапа](#восстановление-данных-из-бекапа)

## Задание

1. Развернуть 2 шарда по 2(3) реплики (не забываем про КС) + 1 Mongos
2. Протестировать создание бэкапов через PBM
3. Не забываем ВМ остановить/удалить

## refs

- [Setting Up and Monitoring MongoDB 8 Replica Sets with PMM 3 Using Docker: A Beginner-Friendly Guide](https://percona.community/blog/2025/03/18/setting-up-and-monitoring-mongodb-8-replica-sets-with-pmm-3-using-docker-a-beginner-friendly-guide/)
- [Run Percona Backup for MongoDB in a Docker container](https://docs.percona.com/percona-backup-mongodb/install/docker.html)
- [Percona Backup for MongoDB Initial setup overview](https://docs.percona.com/percona-backup-mongodb/install/initial-setup.html)

## Решение

### подготовка

В Docker Desktop увеличить ограничение на ресурсы по используемой памяти до 16Gb

Создать файл `docker-compose.yml`

<details>
<summary>содержимое файла `docker-compose.yml`</summary>

```yaml
services:
  mongodb-cfg1:
    profiles: [ cfg, shard ]
    image: percona/percona-server-mongodb:7.0

    container_name: mongodb-cfg1
    ports:
      - "27117:27017"
    volumes:
      - ./docker/mongodb-cfg1:/data/db
      - ./secrets:/etc/secrets:ro
    # command: ["mongod", "--replSet", "rs-cfg", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db"]      
    command: ["mongod", "--configsvr", "--replSet", "rs-cfg", "--auth", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5

  mongodb-cfg2:
    profiles: [ cfg, shard ]
    image: percona/percona-server-mongodb:7.0
    container_name: mongodb-cfg2
    ports:
      - "27217:27017"
    volumes:
      - ./docker/mongodb-cfg2:/data/db
      - ./secrets:/etc/secrets:ro
    # command: ["mongod", "--replSet", "rs-cfg", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db"]      
    command: ["mongod", "--configsvr", "--replSet", "rs-cfg", "--auth", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5

  mongodb-cfg3:
    profiles: [ cfg, shard ]
    image: percona/percona-server-mongodb:7.0
    container_name: mongodb-cfg3
    ports:
      - "27317:27017"
    volumes:
      - ./docker/mongodb-cfg3:/data/db
      - ./secrets:/etc/secrets:ro
    # command: ["mongod", "--replSet", "rs-cfg", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db"]      
    command: ["mongod", "--configsvr", "--replSet", "rs-cfg", "--auth", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongodb-rs1n1:
    profiles: [ rs1, shard ]
    image: percona/percona-server-mongodb:7.0
    container_name: mongodb-rs1n1
    ports:
      - "28117:27017"
    volumes:
      - ./docker/mongodb-rs1n1:/data/db
      - ./secrets:/etc/secrets:ro
    # command: ["mongod", "--replSet", "rs1", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    command: ["mongod", "--shardsvr", "--replSet", "rs1", "--auth", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongodb-rs1n2:
    profiles: [ rs1, shard ]
    image: percona/percona-server-mongodb:7.0
    container_name: mongodb-rs1n2
    ports:
      - "28217:27017"
    volumes:
      - ./docker/mongodb-rs1n2:/data/db
      - ./secrets:/etc/secrets:ro
    # command: ["mongod", "--replSet", "rs1", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    command: ["mongod", "--shardsvr", "--replSet", "rs1", "--auth", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongodb-rs1n3:
    profiles: [ rs1, shard ]
    image: percona/percona-server-mongodb:7.0
    container_name: mongodb-rs1n3
    ports:
      - "28317:27017"
    volumes:
      - ./docker/mongodb-rs1n3:/data/db
      - ./secrets:/etc/secrets:ro
    # command: ["mongod", "--replSet", "rs1", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    command: ["mongod", "--shardsvr", "--replSet", "rs1", "--auth", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongodb-rs2n1:
    profiles: [ rs2, shard ]
    image: percona/percona-server-mongodb:7.0
    container_name: mongodb-rs2n1
    ports:
      - "29117:27017"
    volumes:
      - ./docker/mongodb-rs2n1:/data/db
      - ./secrets:/etc/secrets:ro
    # command: ["mongod", "--replSet", "rs2", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    command: ["mongod", "--shardsvr", "--replSet", "rs2", "--auth", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongodb-rs2n2:
    profiles: [ rs2, shard ]
    image: percona/percona-server-mongodb:7.0
    container_name: mongodb-rs2n2
    ports:
      - "29217:27017"
    volumes:
      - ./docker/mongodb-rs2n2:/data/db
      - ./secrets:/etc/secrets:ro
    # command: ["mongod", "--replSet", "rs2", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    command: ["mongod", "--shardsvr", "--replSet", "rs2", "--auth", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongodb-rs2n3:
    profiles: [ rs2, shard ]
    image: percona/percona-server-mongodb:7.0
    container_name: mongodb-rs2n3
    ports:
      - "29317:27017"
    volumes:
      - ./docker/mongodb-rs2n3:/data/db
      - ./secrets:/etc/secrets:ro
    # command: ["mongod", "--replSet", "rs2", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    command: ["mongod", "--shardsvr", "--replSet", "rs2", "--auth", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongos1:
    profiles: [ mongos, shard ]
    image: percona/percona-server-mongodb:7.0
    container_name: mongos1
    ports:
      - "30117:27017"
    volumes:
      - ./docker/mongos1:/data/db
      - ./secrets:/etc/secrets:ro
    command: ["mongos", "--configdb", "rs-cfg/mongodb-cfg1:27017,mongodb-cfg2:27017,mongodb-cfg3:27017", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--bind_ip_all"]
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password


  pb-cfg1:
    profiles: [ pbcfg, pb ]
    image: percona/percona-backup-mongodb:2.11.0
    container_name: pb-cfg1
    volumes:
      - ./pbm_config.yaml:/etc/pbm_config.yaml:ro
      - ./backup-shared:/data/backup
    environment:
      PBM_MONGODB_URI: "mongodb://pbmuser:secretpwd@mongodb-cfg1:27017"

  pb-cfg2:
    profiles: [ pbcfg, pb ]
    image: percona/percona-backup-mongodb:2.11.0
    container_name: pb-cfg2
    volumes:
      - ./pbm_config.yaml:/etc/pbm_config.yaml:ro
      - ./backup-shared:/data/backup
    environment:
      PBM_MONGODB_URI: "mongodb://pbmuser:secretpwd@mongodb-cfg2:27017"

  pb-cfg3:
    profiles: [ pbcfg, pb ]
    image: percona/percona-backup-mongodb:2.11.0
    container_name: pb-cfg3
    volumes:
      - ./pbm_config.yaml:/etc/pbm_config.yaml:ro
      - ./backup-shared:/data/backup
    environment:
      PBM_MONGODB_URI: "mongodb://pbmuser:secretpwd@mongodb-cfg3:27017"


  pb-rs1n1:
    profiles: [ pbrs1, pb ]
    image: percona/percona-backup-mongodb:2.11.0
    container_name: pb-rs1n1
    volumes:
      - ./pbm_config.yaml:/etc/pbm_config.yaml:ro
      - ./backup-shared:/data/backup
    environment:
      PBM_MONGODB_URI: "mongodb://pbmuser:secretpwd@mongodb-rs1n1:27017"

  pb-rs1n2:
    profiles: [ pbrs1, pb ]
    image: percona/percona-backup-mongodb:2.11.0
    container_name: pb-rs1n2
    volumes:
      - ./pbm_config.yaml:/etc/pbm_config.yaml:ro
      - ./backup-shared:/data/backup
    environment:
      PBM_MONGODB_URI: "mongodb://pbmuser:secretpwd@mongodb-rs1n2:27017"

  pb-rs1n3:
    profiles: [ pbrs1, pb ]
    image: percona/percona-backup-mongodb:2.11.0
    container_name: pb-rs1n3
    volumes:
      - ./pbm_config.yaml:/etc/pbm_config.yaml:ro
      - ./backup-shared:/data/backup
    environment:
      PBM_MONGODB_URI: "mongodb://pbmuser:secretpwd@mongodb-rs1n3:27017"

  pb-rs2n1:
    profiles: [ pbrs2, pb ]
    image: percona/percona-backup-mongodb:2.11.0
    container_name: pb-rs2n1
    volumes:
      - ./pbm_config.yaml:/etc/pbm_config.yaml:ro
      - ./backup-shared:/data/backup
    environment:
      PBM_MONGODB_URI: "mongodb://pbmuser:secretpwd@mongodb-rs2n1:27017"

  pb-rs2n2:
    profiles: [ pbrs2, pb ]
    image: percona/percona-backup-mongodb:2.11.0
    container_name: pb-rs2n2
    volumes:
      - ./pbm_config.yaml:/etc/pbm_config.yaml:ro
      - ./backup-shared:/data/backup
    environment:
      PBM_MONGODB_URI: "mongodb://pbmuser:secretpwd@mongodb-rs2n2:27017"

  pb-rs2n3:
    profiles: [ pbrs2, pb ]
    image: percona/percona-backup-mongodb:2.11.0
    container_name: pb-rs2n3
    volumes:
      - ./pbm_config.yaml:/etc/pbm_config.yaml:ro
      - ./backup-shared:/data/backup
    environment:
      PBM_MONGODB_URI: "mongodb://pbmuser:secretpwd@mongodb-rs2n3:27017"

```

</summary>
</details>

Создать каталог `secrets` и сгенерировать файл `mongodb-keyfile`

```sh
mkdir secrets
openssl rand -base64 128 > secrets/mongodb-keyfile
chmod 600 secrets/mongodb-keyfile
```

Создать файл `pbm_config.yaml`

```yaml
storage:
  type: filesystem
  filesystem:
    path: /data/backup
```

### запуск replica set конфигурации

Запустить контейнеры, относящиеся к реплике конфигурации

```sh
docker compose --profile cfg up -d
```

Сначала без ключа `--configsvr` иначе возникает ошибка

```log
BadValue: Cannot start a configsvr as a standalone server. Please use the option --replSet to start the node as a replica set.
```

После запуска остановить контейнеры, добавить ключ `--configsvr` и запустить контейнеры повторно


Подключиться к первой ноде

```sh
mongosh 127.0.0.1:27117 -u admin -p password --authenticationDatabase admin
```

Выполнить инициализацию реплики

```js
rs.initiate(
  {
    _id: "rs-cfg",
    configsvr: true,
    members: [
      { _id : 0, host : "mongodb-cfg1:27017" },
      { _id : 1, host : "mongodb-cfg2:27017" },
      { _id : 2, host : "mongodb-cfg3:27017" }
    ]
  }
)

rs.status()
```

Создать роль для Percona Backup for MongoDB

```js
db.getSiblingDB("admin").createRole({ "role": "pbmAnyAction",
      "privileges": [
         { "resource": { "anyResource": true },
           "actions": [ "anyAction" ]
         }
      ],
      "roles": []
   });
```

Создать пользователя для ранее созданной роли

```js
db.getSiblingDB("admin").createUser({"user": "pbmuser",
       "pwd": "secretpwd",
       "roles" : [
          { "db" : "admin", "role" : "readWrite", "collection": "" },
          { "db" : "admin", "role" : "backup" },
          { "db" : "admin", "role" : "clusterMonitor" },
          { "db" : "admin", "role" : "restore" },
          { "db" : "admin", "role" : "pbmAnyAction" }
       ]
    });
```

### запуск replica set данных №1

Запустить контейнеры

```sh
docker compose --profile rs1 up -d
```

Подключиться к первой ноде

```sh
mongosh 127.0.0.1:28117 -u admin -p password --authenticationDatabase admin
```

Выполнить инициализацию реплики

```js
rs.initiate(
  {
    _id: "rs1",
    members: [
      { _id : 0, host : "mongodb-rs1n1:27017" },
      { _id : 1, host : "mongodb-rs1n2:27017" },
      { _id : 2, host : "mongodb-rs1n3:27017" }
    ]
  }
)

rs.status()
```

Создать роль для Percona Backup for MongoDB

```js
db.getSiblingDB("admin").createRole({ "role": "pbmAnyAction",
      "privileges": [
         { "resource": { "anyResource": true },
           "actions": [ "anyAction" ]
         }
      ],
      "roles": []
   });
```

Создать пользователя для ранее созданной роли

```js
db.getSiblingDB("admin").createUser({"user": "pbmuser",
       "pwd": "secretpwd",
       "roles" : [
          { "db" : "admin", "role" : "readWrite", "collection": "" },
          { "db" : "admin", "role" : "backup" },
          { "db" : "admin", "role" : "clusterMonitor" },
          { "db" : "admin", "role" : "restore" },
          { "db" : "admin", "role" : "pbmAnyAction" }
       ]
    });
```

### запуск replica set данных №2

Запустить контейнеры

```sh
docker compose --profile rs2 up -d
```

Подключиться к первой ноде

```sh
mongosh 127.0.0.1:29117 -u admin -p password --authenticationDatabase admin
```

Выполнить инициализацию реплики

```js
rs.initiate(
  {
    _id: "rs2",
    members: [
      { _id : 0, host : "mongodb-rs2n1:27017" },
      { _id : 1, host : "mongodb-rs2n2:27017" },
      { _id : 2, host : "mongodb-rs2n3:27017" }
    ]
  }
)

rs.status()
```

Создать роль для Percona Backup for MongoDB

```js
db.getSiblingDB("admin").createRole({ "role": "pbmAnyAction",
      "privileges": [
         { "resource": { "anyResource": true },
           "actions": [ "anyAction" ]
         }
      ],
      "roles": []
   });
```

Создать пользователя для ранее созданной роли

```js
db.getSiblingDB("admin").createUser({"user": "pbmuser",
       "pwd": "secretpwd",
       "roles" : [
          { "db" : "admin", "role" : "readWrite", "collection": "" },
          { "db" : "admin", "role" : "backup" },
          { "db" : "admin", "role" : "clusterMonitor" },
          { "db" : "admin", "role" : "restore" },
          { "db" : "admin", "role" : "pbmAnyAction" }
       ]
    });
```

### запуск mongos и добавление шардов

Запустить контейнер

```sh
docker compose --profile mongos up -d
```

Подключиться к mongos

```sh
mongosh 127.0.0.1:30117 -u admin -p password --authenticationDatabase admin
```

Добавить шарды

```js
use admin

sh.status()

sh.addShard("rs1/mongodb-rs1n1:27017,mongodb-rs1n2:27017,mongodb-rs1n3:27017")
sh.addShard("rs2/mongodb-rs2n1:27017,mongodb-rs2n2:27017,mongodb-rs2n3:27017")

sh.status()
```

### запуск percona-backup-mongodb

[Set up Percona Backup for MongoDB](https://docs.percona.com/percona-backup-mongodb/install/docker.html#set-up-percona-backup-for-mongodb)

> Note that every MongoDB node (including replica set secondary members and config server replica set nodes) requires a separate instance of Percona Backup for MongoDB. Thus, a typical, 3-node MongoDB replica set requires three instances of Percona Backup for MongoDB.

В созданном на этапе подготовки файле `docker-compose.yml` содержатся спецификации контейнеров с percona-backup-mongodb для всех требуемых узлов кластера. 

В каждый контейнер с percona-backup-mongodb

- смонтирован общий каталог для бекапов
- смонтирован на чтение файл с настройками `pbm_config.yaml` (необязательно тк настройки реплицируются между агентами)

Запустить контейнеры с percona-backup-mongodb выполнив команду 

```sh
docker compose --profile pb up -d
```

![all containers](./assets/hw06-all-containers.png)

Подключиться к любому контейнеру с percona-backup-mongodb и выполнить конфигурирование

```sh
docker exec -it pb-rsn1 bash
pbm config --file /etc/pbm_config.yaml

pbm status
```

![pbm init](./assets/hw06-pbm-init.png)

### загрузка тестовых данных

Загрузить тестовые данные в БД

```sh
./mongorestore --host 127.0.0.1 --port 30117 -u admin -p password --authenticationDatabase admin --db finance --collection stocks values.bson
```

![data upload](./assets/hw06-data-upload.png)

Подключиться к mongos

```sh
mongosh 127.0.0.1:30117 -u admin -p password --authenticationDatabase admin
```

Создать индекс и шардировать коллекцию с тестовыми данными

```js
use finance

show collections

db.stocks.countDocuments()
db.stocks.findOne({})

db.stocks.createIndex({stock_symbol: 1})

sh.enableSharding("finance")
sh.status()

sh.shardCollection("finance.stocks",{ stock_symbol: 1 })
sh.status()
```

![data uploaded](./assets/hw06-data-uploaded.png)

Создать первую маркерную запись

```js
db.stocks.insertOne({    
  exchange: 'NONE',
  stock_symbol: 'MARKER',
  date: '2025-10-19',
  open: 1.1,
  high: 2.2,
  low: 3.3,
  close: 4.4,
  volume: 55555,
  'adj close': 6.66
})

db.stocks.find({stock_symbol: 'MARKER'})
```

### создание бекапа

Подключиться к любому контейнеру с percona-backup-mongodb

```sh
docker exec -it pb-rsn1 bash

pbm status
pbm backup
```

Подключиться к mongos

```sh
mongosh 127.0.0.1:30117 -u admin -p password --authenticationDatabase admin
```

Создать вторую маркерную запись

```js
use finance

db.stocks.insertOne({    
  exchange: 'NONE',
  stock_symbol: 'MARKER2',
  date: '2025-10-19',
  open: 0.0,
  high: 0.0,
  low: 0.0,
  close: 0.0,
  volume: 0,
  'adj close': 0
})

db.stocks.find({stock_symbol: 'MARKER2'})
```

### удаление данных

Подключиться к mongos

```sh
mongosh 127.0.0.1:30117 -u admin -p password --authenticationDatabase admin
```

Удалить коллекцию stocks из БД finance

```js
use finance
db.stocks.drop()
sh.status()
```

### восстановление данных из бекапа

Подключиться к любому контейнеру с percona-backup-mongodb

```sh
docker exec -it pb-rsn1 bash

pbm status
pbm list
pbm restore 2025-10-19T13:56:14Z
```

![have backup](./assets/hw06-pbm-have-backup.png)

![list backup](./assets/hw06-pbm-list.png)

![list restore](./assets/hw06-pbm-restore.png)

Подключиться к mongos

```sh
mongosh 127.0.0.1:30117 -u admin -p password --authenticationDatabase admin
```

Проверить наличие коллекции 

```js
use finance
show collections
```

и документов в ней

```js
db.stocks.find({stock_symbol: 'MARKER'})
db.stocks.find({stock_symbol: 'MARKER2'})
```

Первый маркерный документ присутствует тк был создан до начала бекапа и попал в состав сохраняемых документов. Второй маркерный документ ожидаемо отсутствует.
