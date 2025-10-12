# Домашнее задание 3

- [Домашнее задание 3](#домашнее-задание-3)
  - [Задание](#задание)
  - [Решение](#решение)
    - [подготовка](#подготовка)
    - [запуск replica set конфигурации](#запуск-replica-set-конфигурации)
    - [запуск replica set данных №1](#запуск-replica-set-данных-1)
    - [запуск replica set данных №2](#запуск-replica-set-данных-2)
    - [запуск mongos и добавление шардов](#запуск-mongos-и-добавление-шардов)
    - [загрузка тестовых данных](#загрузка-тестовых-данных)
    - [запросы к тестовым данным](#запросы-к-тестовым-данным)
      - [map reduce](#map-reduce)
      - [aggregation framework](#aggregation-framework)
      - [выводы](#выводы)
    - [остановка кластера](#остановка-кластера)
 

## Задание

1. Развернуть ВМ (Linux) с MongoDB (у вас есть ВМ в ВБ, любой другой способ, в т.ч. докер)
2. Построить шардированный кластер из 3(2) кластерных нод (по 3 инстанса с репликацией
(мб арбитр)) и с кластером конфига(3 инстанса);
3. Добавить  балансировку,  нагрузить  данными,  выбрать  хороший  ключ  шардирования, 
посмотреть как данные перебалансируются между шардами (stocks.zip)
4. Написать  Map  Reduce  на  ваше  усмотрение  для  подсчёта  агрегированных  данных 
(Например  аналог  db.values.find({$where:  '(this.open  -  this.close  > 
100)'},{"stock_symbol":1,"open":1,"close":1})  -  заодно  проверьте  и  скорость  стандартного агрегатного запроса)
5. Не забываем ВМ остановить/удалить

## Решение

### подготовка

В Docker Desktop увеличить ограничение на ресурсы по используемой памяти до 12Gb

Создать файл `docker-compose.yml`

<details>
  <summary>содержимое файла `docker-compose.yml`</summary>

```yaml
# shared cluster

services:
  mongodb-cfg1:
    profiles: [ cfg, shard ]
    image: mongodb/mongodb-community-server:8.0-ubuntu2204
    container_name: mongodb-cfg1
    ports:
      - "27117:27017"
    volumes:
      - ./docker/mongodb-cfg1:/data/db
      - ./secrets:/etc/secrets:ro
    # command: ["mongod", "--replSet", "rs-cfg", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db"]      
    command: ["mongod", "--configsvr", "--replSet", "rs-cfg",  "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGODB_INITDB_ROOT_USERNAME: admin
      MONGODB_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5

  mongodb-cfg2:
    profiles: [ cfg, shard ]
    image: mongodb/mongodb-community-server:8.0-ubuntu2204
    container_name: mongodb-cfg2
    ports:
      - "27217:27017"
    volumes:
      - ./docker/mongodb-cfg2:/data/db
      - ./secrets:/etc/secrets:ro
    # command: ["mongod", "--replSet", "rs-cfg", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db"]      
    command: ["mongod", "--configsvr", "--replSet", "rs-cfg", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGODB_INITDB_ROOT_USERNAME: admin
      MONGODB_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5

  mongodb-cfg3:
    profiles: [ cfg, shard ]
    image: mongodb/mongodb-community-server:8.0-ubuntu2204
    container_name: mongodb-cfg3
    ports:
      - "27317:27017"
    volumes:
      - ./docker/mongodb-cfg3:/data/db
      - ./secrets:/etc/secrets:ro
    # command: ["mongod", "--replSet", "rs-cfg", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db"]      
    command: ["mongod", "--configsvr", "--replSet", "rs-cfg", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGODB_INITDB_ROOT_USERNAME: admin
      MONGODB_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5            


  mongodb-rs1n1:
    profiles: [ rs1, shard ]
    image: mongodb/mongodb-community-server:8.0-ubuntu2204
    container_name: mongodb-rs1n1
    ports:
      - "28117:27017"
    volumes:
      - ./docker/mongodb-rs1n1:/data/db
      - ./secrets:/etc/secrets:ro
    command: ["mongod", "--shardsvr", "--replSet", "rs1", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGODB_INITDB_ROOT_USERNAME: admin
      MONGODB_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongodb-rs1n2:
    profiles: [ rs1, shard ]
    image: mongodb/mongodb-community-server:8.0-ubuntu2204
    container_name: mongodb-rs1n2
    ports:
      - "28217:27017"
    volumes:
      - ./docker/mongodb-rs1n2:/data/db
      - ./secrets:/etc/secrets:ro
    command: ["mongod", "--shardsvr", "--replSet", "rs1", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGODB_INITDB_ROOT_USERNAME: admin
      MONGODB_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongodb-rs1n3:
    profiles: [ rs1, shard ]
    image: mongodb/mongodb-community-server:8.0-ubuntu2204
    container_name: mongodb-rs1n3
    ports:
      - "28317:27017"
    volumes:
      - ./docker/mongodb-rs1n3:/data/db
      - ./secrets:/etc/secrets:ro
    command: ["mongod", "--shardsvr", "--replSet", "rs1", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGODB_INITDB_ROOT_USERNAME: admin
      MONGODB_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongodb-rs2n1:
    profiles: [ rs2, shard ]
    image: mongodb/mongodb-community-server:8.0-ubuntu2204
    container_name: mongodb-rs2n1
    ports:
      - "29117:27017"
    volumes:
      - ./docker/mongodb-rs2n1:/data/db
      - ./secrets:/etc/secrets:ro
    command: ["mongod", "--shardsvr", "--replSet", "rs2", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGODB_INITDB_ROOT_USERNAME: admin
      MONGODB_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongodb-rs2n2:
    profiles: [ rs2, shard ]
    image: mongodb/mongodb-community-server:8.0-ubuntu2204
    container_name: mongodb-rs2n2
    ports:
      - "29217:27017"
    volumes:
      - ./docker/mongodb-rs2n2:/data/db
      - ./secrets:/etc/secrets:ro
    command: ["mongod", "--shardsvr", "--replSet", "rs2", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGODB_INITDB_ROOT_USERNAME: admin
      MONGODB_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongodb-rs2n3:
    profiles: [ rs2, shard ]
    image: mongodb/mongodb-community-server:8.0-ubuntu2204
    container_name: mongodb-rs2n3
    ports:
      - "29317:27017"
    volumes:
      - ./docker/mongodb-rs2n3:/data/db
      - ./secrets:/etc/secrets:ro
    command: ["mongod", "--shardsvr", "--replSet", "rs2", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all"]
    environment:
      MONGODB_INITDB_ROOT_USERNAME: admin
      MONGODB_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongos1:
    profiles: [ mongos, shard ]
    image: mongodb/mongodb-community-server:8.0-ubuntu2204
    container_name: mongos1
    ports:
      - "30117:27017"
    volumes:
      - ./docker/mongos1:/data/db
      - ./secrets:/etc/secrets:ro
    command: ["mongos", "--configdb", "rs-cfg/mongodb-cfg1:27017,mongodb-cfg2:27017,mongodb-cfg3:27017", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--bind_ip_all"]
    environment:
      MONGODB_INITDB_ROOT_USERNAME: admin
      MONGODB_INITDB_ROOT_PASSWORD: password
   
```
</details>


Создать каталог `secrets` и сгенерировать файл `mongodb-keyfile`

```sh
mkdir secrets
openssl rand -base64 128 > secrets/mongodb-keyfile
chmod 600 secrets/mongodb-keyfile
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

### загрузка тестовых данных

Скачать утилиту mongorestore для mac [MongoDB Command Line Database Tools Download](https://www.mongodb.com/try/download/database-tools)

Скачать тестовые данные

```sh
wget https://dl.dropboxusercontent.com/s/p75zp1karqg6nnn/stocks.zip
```

Загрузить тестовые данные в БД

```sh
./mongorestore --host 127.0.0.1 --port 30117 -u admin -p password --authenticationDatabase admin --db test --collection stocks values.bson
```

Подключиться к `mongos`

```sh
mongosh 127.0.0.1:30117 -u admin -p password --authenticationDatabase admin
```

Создать индекс и шардировать коллекцию с тестовыми данными

```js
use test

show collections

db.stocks.countDocuments()
db.stocks.findOne({})

db.stocks.createIndex({stock_symbol: 1})
// статус создания индекса
db.currentOp(true).inprog.forEach(function(op){ if(op.msg!==undefined) print(op.msg) })
db.stocks.dropIndex({stock_symbol: 1})
db.stocks.getIndexes()

sh.enableSharding("test")
sh.status()

sh.shardCollection("test.stocks",{ stock_symbol: 1 })
sh.status()
```

На текущий момент кластер полностью развернут

![cluster containers](./assets/hw04-containers.png)

Коллекция `test.stocks` шардирована

![sharded collection](./assets/hw04-sharded-collection.png)

### запросы к тестовым данным

#### map reduce

Выполнить запрос

```js
var mapFunction1 = function() {
  var delta = this.open - this.close
  if (delta > 100) {
    emit(
      this._id,
      {
        stock_symbol: this.stock_symbol,
        open: this.open,
        close: this.close
      }
    );
  }
};

var reduceFunction1 = function(key, values) {
   return values[0];
};

db.stocks.mapReduce(
   mapFunction1,
   reduceFunction1,
   { out: "map_reduce_result" }
)

show collections
db.map_reduce_result.findOne({})
db.map_reduce_result.countDocuments()
db.map_reduce_result.drop()
```

Замерить время выполнения. В данном случае замеряется клиентское время

```js
function time(command) {
    const t1 = new Date();
    const result = command();
    const t2 = new Date();
    print("time: " + (t2 - t1) + "ms");
    return result; 
}

time(
  () => db.stocks.mapReduce(
   mapFunction1,
   reduceFunction1,
   { out: "map_reduce_result" }
  )
)
```

Результат для трех запусков: `time: 5345ms` / `time: 5402ms` / `time: 5008ms`

#### aggregation framework

Выполнить запрос

```js
db.stocks.aggregate(
    [
        {
          $addFields: {
            delta: { $subtract: ["$open", "$close"] }
          }
        },
        {
          $match: {"delta": {$gt: 100}}
        },
        { 
          $project: {"stock_symbol":1,"open":1,"close":1}
        }
    ]
)
```

Замерить время выполнения

```js
function time(command) {
    const t1 = new Date();
    const result = command();
    const t2 = new Date();
    print("time: " + (t2 - t1) + "ms");
    return result; 
}

time(
  () => db.stocks.aggregate(
    [
        {
          $addFields: {
            delta: { $subtract: ["$open", "$close"] }
          }
        },
        {
          $match: {"delta": {$gt: 100}}
        },
        { 
          $project: {"_id": 0, "stock_symbol":1,"open":1,"close":1}
        }
    ]
  ).toArray()
)
```

Result `time: 2697ms` / `time: 2631ms` / `time: 2729ms`


#### выводы

Предположу что более высокая скорость выполнения запроса с использованием aggregation pipeline (среднее около 2700ms) по сравнению с map reduce (среднее около 5300ms) обусловлена тем что map reduce помимо получения результатов еще и сохраняет их в коллекцию.

### остановка кластера

```sh
docker compose --profile shard stop
```
