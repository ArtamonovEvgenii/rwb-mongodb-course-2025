# Домашнее задание 5

- [Домашнее задание 5](#домашнее-задание-5)
  - [Задание](#задание)
  - [Решение](#решение)
    - [подготовка](#подготовка)
    - [аутентификация](#аутентификация)
    - [валидация](#валидация)

## Задание

1. Развернуть ВМ (Linux) с MongoDB (у вас есть ВМ в ВБ, любой другой способ, в т.ч. докер)
2. Настроить аутентификацию и валидацию
3. Проверить как работает при экспорте/импорте данных
a. список датасетов для загрузки (выбрать 1) - https://github.com/jdorfman/awesome-json-datasets 
4. Не забываем ВМ остановить/удалить

## Решение

### подготовка

Создать файл `docker-compose.yml`

<details>
<summary>содержимое файла `docker-compose.yml`</summary>

```yaml
services:
  mongodb:
    image: mongodb/mongodb-community-server:8.0-ubuntu2204
    container_name: mongodb
    ports:
      - "27017:27017"
    volumes:
      - ./docker/mongodb-single:/data/db
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
    healthcheck:
      test: ["CMD-SHELL", "mongosh --eval 'db.adminCommand(\"ping\")' --quiet"]
      interval: 30s
      timeout: 10s
      retries: 5      
```

</summary>
</details>

Запустить контейнер выполнив команду

```sh
docker compose up -d
```

### аутентификация

Подключиться к серверу БД

```sh
mongosh 127.0.0.1:27017 -u admin -p password --authenticationDatabase admin
```

Создать пользователей для БД `rickandmorty`

| имя пользователя | роль      | БД аутентификации |
| ---------------- | --------- | ----------------- |
| writer           | readWrite | admin             |
| reader           | read      | rickandmorty      |


```sh
use rickandmorty

dbAdmin = db.getSiblingDB("admin")

dbAdmin.createUser({
     user: "writer",      
     pwd: "writerPassword",
     roles: [{role: "readWrite", db: "rickandmorty"}]
})

db.createUser({
     user: "reader",      
     pwd: "readerPassword",
     roles: [{role: "read", db: "rickandmorty"}]
})

use admin
db.system.users.find({})

```

Попробовать подключиться пользователем `writer` используя некорректную БД для аутентификации

```sh
mongosh 127.0.0.1:27017 -u writer -p writerPassword --authenticationDatabase rickandmorty
```

![writer connection failed](./assets/hw05-auth-01.png)

Подключиться пользователем `writer` используя корректную БД для аутентификации

```sh
mongosh 127.0.0.1:27017 -u writer -p writerPassword --authenticationDatabase admin
```

Проверить доступность записи и чтения в БД `rickandmorty`

```sh
use rickandmorty

db.testaccess.insertOne({"foo": "bar"})
db.testaccess.find({})
```

![writer access](./assets/hw05-auth-02.png)

Запись и чтение доступны что соответствует роли пользователя

Попробовать подключиться пользователем `reader` используя некорректную БД для аутентификации

```sh
mongosh 127.0.0.1:27017 -u reader -p readerPassword --authenticationDatabase admin
```

![reader connection failed](./assets/hw05-auth-03.png)

Подключиться пользователем `reader` используя корректную БД для аутентификации

```sh
mongosh 127.0.0.1:27017 -u reader -p readerPassword --authenticationDatabase rickandmorty
```

```sh
use rickandmorty

db.testaccess.insertOne({"foo": "bar"})
db.testaccess.find({})
```

![reader access](./assets/hw05-auth-04.png)

Запись недоступна, чтение доступно что соответствует роли пользователя

### валидация

Из предложенного [набора датасетов](https://github.com/jdorfman/awesome-json-datasets)  выбран датасет с персонажами из мультфильма `Rick and Morty`.

Скачать [файл](https://rickandmortyapi.com/api/character/) и сохранить под именем `characters-origin.json`.

```sh
curl -o characters-origin.json https://rickandmortyapi.com/api/character/
```

Подготовить формат файла для загрузки с помощью утилиты `mongoimport`. Файл должен содежать массив объектов.

```sh
jq .results characters-origin.json > characters.json
```

Подключиться к серверу БД

```sh
mongosh 127.0.0.1:27017 -u admin -p password --authenticationDatabase admin
```

Создать коллекцию со строгими правилами валидации документов используя режимы

- `validationLevel`: `strict`
- `validationAction`: `error`

```js
db.createCollection( 
    "characters",
    {
        validator: {
            $jsonSchema : {
                bsonType: "object",
                required: [ "name", "species", "gender" ],
                properties: {
                    name: {
                        bsonType: "string",
                        description: "required non empty string"
                    },
                    species: {
                        bsonType: "string",
                        description: "required non empty string"
                    },
                    gender: {
                        bsonType: "string",
                        enum: [ "Male", "Female" ],
                        description: "required and must be Male or Female"
                    }
                }
            }
        },
        validationLevel: "strict",
        validationAction: "error"
    }
)
```

Загрузить данные из файла в БД выполнив

```sh
./mongoimport --host 127.0.0.1 --port 27017 -u admin -p password --authenticationDatabase admin --db rickandmorty --collection characters --mode upsert --upsertFields name --file ./characters.json --jsonArray
```

![upload with strict validation](./assets/hw05-val-01.png)

Были загружены 19 документов из 20

Понизить строгость валидации установив режимы

- `validationLevel`: `moderate`
- `validationAction`: `warn`

```js
db.runCommand({
    collMod: "characters",
    validator: {
        $jsonSchema : {
            bsonType: "object",
            required: [ "name", "species", "gender" ],
            properties: {
                name: {
                    bsonType: "string",
                    description: "required non empty string"
                },
                species: {
                    bsonType: "string",
                    description: "required non empty string"
                },
                gender: {
                    bsonType: "string",
                    enum: [ "Male", "Female" ],
                    description: "required and must be Male or Female"
                }
            }
        }
    },
    validationLevel: "moderate",
    validationAction: "warn"
})
```

Повторно загрузить данные из файла в БД выполнив

Обратить внимание на режим загрузки `upsert` по полю `name` для предотвращения дублирования данных

```sh
./mongoimport --host 127.0.0.1 --port 27017 -u admin -p password --authenticationDatabase admin --db rickandmorty --collection characters --mode upsert --upsertFields name --file ./characters.json --jsonArray
```

Были загружены все 20 документов

![upload with strict validation](./assets/hw05-val-02.png)

Проверить логи выполнив команду 

```sh
docker compose logs mongodb
```

Найти в логах записи об ошибках валидации

![upload with strict validation](./assets/hw05-val-03.png)

Проверить наличие проблем в документе выполнив поиск 

```js
use rickandmorty
db.characters.find({name: "Alien Googah"})
```

![upload with strict validation](./assets/hw05-val-04.png)

Действительно документ содержит значение поля `gender` равное `unknown` отлично от допустимых `Male`, `Female`
