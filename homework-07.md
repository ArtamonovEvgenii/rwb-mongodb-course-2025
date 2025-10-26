# Домашнее задание 7

- [Домашнее задание 7](#домашнее-задание-7)
  - [Задание](#задание)
  - [Решение](#решение)
    - [подготовка](#подготовка)
    - [запуск replica set](#запуск-replica-set)
    - [запуск percona monitoring and management](#запуск-percona-monitoring-and-management)
    - [запуск percona monitoring and management client](#запуск-percona-monitoring-and-management-client)
    - [загрузка тестовых данных](#загрузка-тестовых-данных)
    - [запуск сервиса API к БД](#запуск-сервиса-api-к-бд)
    - [нагрузочное тестирование](#нагрузочное-тестирование)

## Задание

1. Развернуть ВМ (Linux) с MongoDB (у вас есть ВМ в ВБ, любой другой способ, в т.ч. докер)
2. Настроить  мониторинг  через  PMM  -  протестировать  на  нагрузке  (любым  удобным 
способом), приложить пару скриншотов
3. Не забываем ВМ остановить/удалить


## Решение

### подготовка

Создать файл `docker-compose.yml`

<details>
<summary>содержимое файла `docker-compose.yml`</summary>

```yaml
services:

  pmm-server:
    # https://127.0.0.1 (443 port)
    # Use admin/admin as the username and password to log in
    profiles: [ pmm ]
    image: percona/pmm-server:3
    # platform: "linux/amd64" # Specifies that Docker should use the image for the amd64 architecture, which is necessary if the container doesn't support ARM and your host system is ARM (e.g., Mac with Apple Silicon).
    container_name: pmm-server
    ports:
      - 8080:80
      - 443:8443
    volumes:
      - ./docker/pmm-data:/srv  
    healthcheck: # Defines a command to check the container's health and sets the timing for executions and retries.
      test: ["CMD-SHELL", "curl -k -f -L https://pmm-server:8443 > /dev/null 2>&1 || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 5

  mongodb-rsn1:
    profiles: [ rs ]
    image: percona/percona-server-mongodb:7.0
    container_name: mongodb-rsn1
    ports:
      - "27117:27017"
    volumes:
      - ./docker/mongodb-rsn1:/data/db
      - ./secrets:/etc/secrets:ro
    command: ["mongod", "--replSet", "rs", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all", "--profile", "2", "--slowms", "200", "--rateLimit", "100"]
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongodb-rsn2:
    profiles: [ rs ]
    image: percona/percona-server-mongodb:7.0
    container_name: mongodb-rsn2
    ports:
      - "27217:27017"
    volumes:
      - ./docker/mongodb-rsn2:/data/db
      - ./secrets:/etc/secrets:ro
    command: ["mongod", "--replSet", "rs", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all", "--profile", "2", "--slowms", "200", "--rateLimit", "100"]
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  mongodb-rsn3:
    profiles: [ rs ]
    image: percona/percona-server-mongodb:7.0
    container_name: mongodb-rsn3
    ports:
      - "27317:27017"
    volumes:
      - ./docker/mongodb-rsn3:/data/db
      - ./secrets:/etc/secrets:ro
    command: ["mongod", "--replSet", "rs", "--keyFile", "/etc/secrets/mongodb-keyfile", "--port", "27017", "--dbpath", "/data/db", "--bind_ip_all", "--profile", "2", "--slowms", "200", "--rateLimit", "100"]
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5


  pmm-client:
    profiles: [ pmmc ]
    image: percona/pmm-client:3
    container_name: pmm-client
    environment:
      PMM_AGENT_SERVER_ADDRESS: pmm-server:8443
      PMM_AGENT_SERVER_USERNAME: admin
      PMM_AGENT_SERVER_PASSWORD: admin
      PMM_AGENT_SERVER_INSECURE_TLS: 1
      PMM_AGENT_CONFIG_FILE: config/pmm-agent.yaml
      PMM_AGENT_SETUP: 1
      PMM_AGENT_SETUP_FORCE: 1
      PMM_AGENT_PRERUN_SCRIPT: >
        pmm-admin status --wait=10s &&
        pmm-admin add mongodb --service-name=mongodb-rsn1 --username=admin --password=password --host=mongodb-rsn1 --port=27017 --query-source=profiler &&
        pmm-admin add mongodb --service-name=mongodb-rsn2 --username=admin --password=password --host=mongodb-rsn2 --port=27017 --query-source=profiler &&
        pmm-admin add mongodb --service-name=mongodb-rsn3 --username=admin --password=password --host=mongodb-rsn3 --port=27017 --query-source=profiler

  gomongoclient:
    profiles: [ gmc ]
    image: gomongoclient:1.0
    container_name: gomongoclient
    environment:
      MONGODB_URI: mongodb://admin:password@mongodb-rsn1:27017/finance?authSource=admin
      MONGODB_NAME: finance
      MONGODB_STOCKS_COLLECTION_NAME: stocks
    ports:
      - 8888:8888

```

</summary>
</details>

Создать каталог `secrets` и сгенерировать файл `mongodb-keyfile`

```sh
mkdir secrets
openssl rand -base64 128 > secrets/mongodb-keyfile
chmod 600 secrets/mongodb-keyfile
```

### запуск replica set

Запустить контейнеры

```sh
docker compose --profile rs up -d
```

Подключиться к первой ноде

```sh
mongosh 127.0.0.1:27117 -u admin -p password --authenticationDatabase admin
```

Выполнить инициализацию реплики

```js
rs.initiate(
  {
    _id: "rs",
    members: [
      { _id : 0, host : "mongodb-rsn1:27017" },
      { _id : 1, host : "mongodb-rsn2:27017" },
      { _id : 2, host : "mongodb-rsn3:27017" }
    ]
  }
)

rs.status()
```

### запуск percona monitoring and management

Запустить контейнер выполнив команду

```sh
docker compose --profile pmm up -d
```

Подключиться к web админке `https://127.0.0.1`

![pmm empty](./assets/hw07-pmm-01.png)

### запуск percona monitoring and management client

Запустить контейнер выполнив команду

```sh
docker compose --profile pmmc up -d
```

С помощью web админки percona monitoring and management убедиться что метрики собираются

![pmm collect data 1](./assets/hw07-pmm-02.png)

![pmm collect data 2](./assets/hw07-pmm-03.png)

![pmm collect data 3](./assets/hw07-pmm-04.png)

### загрузка тестовых данных

Загрузить тестовые данные в БД

```sh
./mongorestore --host 127.0.0.1 --port 27117 -u admin -p password --authenticationDatabase admin --db finance --collection stocks values.bson
```

![upload data](./assets/hw07-upload-data-01.png)

Подключиться к первой ноде

```sh
mongosh 127.0.0.1:27117 -u admin -p password --authenticationDatabase admin
```

```js
use finance
show collections
db.stocks.countDocuments()
```

![upload data](./assets/hw07-upload-data-02.png)

### запуск сервиса API к БД

На языке golang реализован простой сервис. Обрабатывет запрос на эндпойнт `/high`

Тело запроса

```json
{
  "high": 1.123456
}
```

Сервис обращается в базу данных `finance` в коллекцию `stocks` и ищет первый документ у которого атрибут `high` равен заданному. Если документ найден то возвращается код 200 и тело `stockSymbol: AACC`, в случае если не найден код 404 и тело `stockSymbol: not found`

Код сервиса в каталоге `gomongoclient`

Собрать docker image сервиса выполнив команду

```sh
make build-docker
```

Запустить сервис выполнив команду 

```sh
docker compose --profile gmc up -d
```

![upload data](./assets/hw07-docker.png)

### нагрузочное тестирование

В качестве инструмента для нагрузочного тестирования выбран [k6](https://k6.io/)

Подготовлен скрипт `gomongoclient.js`

<details>
<summary>содержимое файла `gomongoclient.js`</summary>

```js
import http from 'k6/http';
import { check, fail, sleep } from 'k6';

export const options = {
    stages: [
        { duration: '3s', target: 5 },
        { duration: '5s', target: 10 },
        { duration: '5s', target: 20 },
        { duration: '5s', target: 50 },
        { duration: '5s', target: 100 },
    ],
};

export default function () {
    let stockHighPool = [
        100.72, // exist
        15.42,  // exist
        71.2,  // exist
        1.123456 // not exist
    ]

    let randomStockHigh = stockHighPool[Math.floor(Math.random()*stockHighPool.length)]

    let data = {
        "high": randomStockHigh
    };

    let url = "http://127.0.0.1:8888/high";
    let res = http.post(url, JSON.stringify(data), {
        headers: { 'Content-Type': 'application/json' },
    });    

    check(res, { 'http status was 200': (r) => r.status === 200 });
    check(res, { 'http status was 404': (r) => r.status === 404 });

    sleep(1);
}
```

</summary>
</details>

Запустить нагрузочное тестирование выполнив команду 

```sh
k6 run gomongoclient.js
```

![k6 stage1 01](./assets/hw07-k6-st1-01.png)

![k6 stage1 02](./assets/hw07-k6-st1-02.png)

Результаты:

- `p(95)`=16,2s
- мониторинг показывает максимум в 12 операций в секунду

Создать индекс для улучшения плана выполнения запроса выполнив команду

```js
db.stocks.createIndex({high: 1})
```

![upload data](./assets/hw07-index.png)

Повторно запустить нагрузочное тестирование выполнив команду 

```sh
k6 run gomongoclient.js
```

![k6 stage2 01](./assets/hw07-k6-st2-01.png)

![k6 stage2 02](./assets/hw07-k6-st2-02.png)

Результаты:

- `p(95)`=4ms
- мониторинг показывает максимум в 63 операции в секунду
