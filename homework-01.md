# Домашнее задание 1

- [Домашнее задание 1](#домашнее-задание-1)
  - [Задание](#задание)
  - [Решение](#решение)
 

## Задание

1. Развернуть ВМ (Linux) с MongoDB (у вас есть ВМ в ВБ, любой другой способ, в т.ч. докер)
2. Создать коллекцию со случайным количеством элементов
3. Посчитать количество элементов
4. Не забываем ВМ остановить/удалить

## Решение

Создать файл `docker-compose.yml` 

```yaml
services:
  mongodb:
    image: mongodb/mongodb-community-server:8.0-ubuntu2204
    container_name: mongodb
    ports:
      - "27017:27017"
    volumes:
      - ./docker/mongodb:/data/db
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
```

Запустить контейнер выполнив команду 

```sh
docker compose up -d
```

Установить консольный клиент mongosh используя homebrew [homebrew tap](https://github.com/mongodb/homebrew-brew)

```sh
brew tap mongodb/brew
brew install mongosh
```

Подключиться к серверу БД

```sh
mongosh 127.0.0.1:27017 -u admin -p password --authenticationDatabase admin
```

Создать коллекцию со случайным количеством документов выполнив команду в клиенте `mongosh`

```js
function randomEmployee() {
    let names = ["Avery","Blake","Aubrey","Alex","Arden","Adrian","Ashley","Blair","Briar","Brooks","Cameron","Carey"]

    let positions = ["Software Developer","Web Developer","Cloud Engineer","DevOps Engineer","Database Administrator","Cybersecurity Specialist","Data Scientist","Business Analyst","Systems Analyst","Support Specialist","Quality Assurance Tester","Project Manager"]

    let employee = {
        age: Math.floor(Math.random() * 60) + 20,
        sex: Math.random() < 0.5 ? "M": "F",
        name: names[Math.floor(Math.random()*names.length)],
        position: positions[Math.floor(Math.random()*positions.length)]
    };

    return employee;
}

let employees = []

for ( i = 0; i < Math.random()*100; ++i ) {
    employees.push(randomEmployee());
}

db.employees.insertMany(
    employees,
    { ordered: false }
)
```

Подсчитать количество документов в колллекции выполнив команду в клиенте `mongosh`

```js
// deprecated
db.employees.find().count()
// actual
db.employees.countDocuments()
```

Остановить контейнер выполнив команду 

```sh
docker compose stop
```
