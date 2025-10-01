# Домашнее задание 2

- [Домашнее задание 2](#домашнее-задание-2)
  - [Задание](#задание)
  - [Решение](#решение)
 

## Задание

1. Развернуть ВМ (Linux) с MongoDB (у вас есть ВМ в ВБ, любой другой способ, в т.ч. докер)
2. Создать 2 коллекции (10-20 строк) с товарами и складами их наличием
3. Или  написать  Map  Reduce  по  подсчёту  количества  товаров  по  наименованиям 
(просуммировать с разных складов)
4. Или реализовать на aggregation framework
5. Не забываем ВМ остановить/удалить

## Решение

> Создание, запуск и подключение к docker контейнеру описаны в домашнем задании №1

Создать коллекцию с товарами выполнив команду в клиенте `mongosh`

```js
db.goods.insertMany(
    [
        {name: "Smartphone", sku: "A1", price: 100},
        {name: "Laptop", sku: "A2", price: 1000},
        {name: "Tablet", sku: "A3", price: 500},
        {name: "Smartwatch", sku: "A4", price: 150},
        {name: "Wireless Earbud", sku: "A5", price: 80},
        {name: "Gaming Console", sku: "A6", price: 200},
        {name: "Smart TV", sku: "A7", price: 140},
        {name: "GPS Receiver ", sku: "A8", price: 50},
        {name: "Digital Camera", sku: "A9", price: 120},
        {name: "Philosopher's Stone", sku: "A10", price: 1000000}
    ],
    { ordered: false }
)
```

Создать коллекцию со складами выполнив команду в клиенте `mongosh`

```js
function randomQuantity() {
    return Math.floor(Math.random() * 100) + 1
}

db.warehouses.insertMany(
    [
        {warehouse: "center", sku: "A1", quantity: randomQuantity()},
        {warehouse: "center", sku: "A2", quantity: randomQuantity()},
        {warehouse: "center", sku: "A3", quantity: randomQuantity()},
        {warehouse: "center", sku: "A4", quantity: randomQuantity()},
        {warehouse: "center", sku: "A5", quantity: randomQuantity()},
        {warehouse: "center", sku: "A6", quantity: randomQuantity()},
        {warehouse: "center", sku: "A7", quantity: randomQuantity()},
        {warehouse: "center", sku: "A8", quantity: randomQuantity()},
        {warehouse: "center", sku: "A9", quantity: randomQuantity()},
        {warehouse: "east", sku: "A3", quantity: randomQuantity()},
        {warehouse: "east", sku: "A4", quantity: randomQuantity()},
        {warehouse: "east", sku: "A5", quantity: randomQuantity()},
        {warehouse: "east", sku: "A6", quantity: randomQuantity()},
        {warehouse: "east", sku: "A7", quantity: randomQuantity()},
        {warehouse: "west", sku: "A6", quantity: randomQuantity()},
        {warehouse: "west", sku: "A7", quantity: randomQuantity()},
        {warehouse: "west", sku: "A8", quantity: randomQuantity()},
        {warehouse: "west", sku: "A9", quantity: randomQuantity()},
    ],
    { ordered: false }
)
```

Подсчитать  количество  товаров  по  наименованиям просуммировав с разных складов используя  aggregation framework

```js
db.goods.aggregate(
    [
        {
            $lookup: {
                from: "warehouses",
                localField: "sku",
                foreignField: "sku",
                as: "warehouses",
                pipeline: [ 
                    { $project: { _id: 0, warehouse: 1, quantity: 1 } }
                ]
            }
        },
        {
            $unwind:
            {
                path: "$warehouses",
                preserveNullAndEmptyArrays: true
            }
        },
        {
            $group:
            {
                _id: "$name",
                totalQuantity: {"$sum": "$warehouses.quantity"},
                warehouses: {"$push": "$warehouses.warehouse"}
            }
        }
    ]
)
```

Пример результата

![Aggregation framework result](./assets/hw02-af-res.png)

> Остановка docker контейнера описана в домашнем задании №1

