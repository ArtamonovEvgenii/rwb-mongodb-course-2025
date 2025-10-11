# Домашнее задание 3

- [Домашнее задание 3](#домашнее-задание-3)
  - [Задание](#задание)
  - [Решение](#решение)
 

## Задание

1. Развернуть ВМ (Linux) с MongoDB (у вас есть ВМ в ВБ, любой другой способ, в т.ч. докер)
2. Создать коллекцию (~20 строк) с товарами и их характеристиками
3. Создать wildtext индекс, проверить работу и проанализировать план запроса
4. Не забываем ВМ остановить/удалить

## Решение

> Создание, запуск и подключение к docker контейнеру описаны в домашнем задании №1

Создать коллекцию с товарами выполнив команду в клиенте `mongosh`

<details>
  <summary>команда создания коллекции с товарами</summary>

```js
db.goods.insertMany(
    [
        {
            "name": "Смартфон Samsung Galaxy S24",
            "sku": "SM-S24-128GB-BLK",
            "description": "Флагманский смартфон с 6.2'' Dynamic AMOLED дисплеем, процессором Exynos 2400 и 128 ГБ памяти.",
            "price": 89990.0
        },
        {
            "name": "Ноутбук Apple MacBook Air M2 13\"",
            "sku": "MBA-M2-13-256GB-SLV",
            "description": "Тонкий и лёгкий ноутбук с чипом Apple M2, 8 ГБ ОЗУ и SSD 256 ГБ. Идеален для работы и учёбы.",
            "price": 129990.0
        },
        {
            "name": "Беспроводные наушники Sony WH-1000XM5",
            "sku": "SONY-WH1000XM5-BLK",
            "description": "Премиальные Bluetooth-наушники с активным шумоподавлением и поддержкой LDAC.",
            "price": 32990.0
        },
        {
            "name": "Смарт-часы Apple Watch Series 9 41мм",
            "sku": "AW-S9-41MM-MIDNIGHT",
            "description": "Умные часы с дисплеем Retina, чипом S9 и функцией отслеживания здоровья.",
            "price": 47990.0
        },
        {
            "name": "Телевизор LG OLED55C4",
            "sku": "LG-OLED55C4-4K",
            "description": "55-дюймовый OLED телевизор с разрешением 4K UHD, поддержкой HDR10 и Dolby Vision.",
            "price": 139990.0
        },
        {
            "name": "Игровая консоль Sony PlayStation 5 Slim",
            "sku": "PS5-SLIM-DISC",
            "description": "Консоль нового поколения с дисководом, поддержкой 4K и 1 ТБ SSD.",
            "price": 65990.0
        },
        {
            "name": "Беспроводная колонка JBL Charge 5",
            "sku": "JBL-CHARGE5-BLU",
            "description": "Портативная Bluetooth-колонка с защитой IP67 и временем работы до 20 часов.",
            "price": 14990.0
        },
        {
            "name": "Монитор Samsung Odyssey G5 32\"",
            "sku": "SAMSUNG-ODYSSEY-G5-32",
            "description": "Изогнутый игровой монитор 32\" с разрешением QHD и частотой обновления 165 Гц.",
            "price": 29990.0
        },
        {
            "name": "Планшет Xiaomi Pad 6",
            "sku": "XIAOMI-PAD6-128GB-GRY",
            "description": "Планшет с 11-дюймовым дисплеем 2.8K, процессором Snapdragon 870 и 128 ГБ памяти.",
            "price": 29990.0
        },
        {
            "name": "Экшн-камера GoPro Hero 12 Black",
            "sku": "GOPRO-HERO12-BLK",
            "description": "Компактная камера с 5.3K-видеозаписью, стабилизацией и защитой от воды.",
            "price": 44990.0
        },
        {
            "name": "Роутер TP-Link Archer AX55",
            "sku": "TPLINK-AX55",
            "description": "Wi-Fi 6 маршрутизатор со скоростью до 3000 Мбит/с и четырьмя антеннами.",
            "price": 8990.0
        },
        {
            "name": "Игровая мышь Logitech G502 HERO",
            "sku": "LOGI-G502-HERO",
            "description": "Проводная игровая мышь с сенсором 25K DPI и 11 программируемыми кнопками.",
            "price": 6490.0
        },
        {
            "name": "Клавиатура механическая Keychron K8",
            "sku": "KEYCHRON-K8-RGB",
            "description": "Компактная беспроводная механическая клавиатура с RGB-подсветкой и свитчами Gateron.",
            "price": 10990.0
        },
        {
            "name": "Умная колонка Яндекс Станция 2",
            "sku": "YANDEX-STATION2",
            "description": "Голосовой помощник с Алисой, поддержкой Spotify и управлением умным домом.",
            "price": 17990.0
        },
        {
            "name": "Фитнес-браслет Xiaomi Smart Band 9",
            "sku": "XIAOMI-SB9-BLK",
            "description": "Фитнес-трекер с AMOLED-дисплеем, пульсометром и временем работы до 14 дней.",
            "price": 4990.0
        },
        {
            "name": "Портативный аккумулятор Anker PowerCore 20000",
            "sku": "ANKER-PC20000",
            "description": "Внешний аккумулятор на 20000 мА·ч с поддержкой Power Delivery и Quick Charge.",
            "price": 5990.0
        },
        {
            "name": "Фотоаппарат Canon EOS R50 Kit",
            "sku": "CANON-R50-KIT",
            "description": "Беззеркальная камера 24.2 Мп с объективом RF-S 18-45mm, Wi-Fi и 4K-видео.",
            "price": 94990.0
        },
        {
            "name": "Проектор XGIMI Horizon Pro",
            "sku": "XGIMI-HORIZONPRO",
            "description": "Домашний 4K проектор с Android TV, автофокусом и яркостью 2200 ANSI люмен.",
            "price": 139990.0
        },
        {
            "name": "Умная лампа Philips Hue White & Color",
            "sku": "PHILIPS-HUE-WC",
            "description": "Светодиодная лампа с возможностью регулировки цвета и интеграцией в умный дом.",
            "price": 3990.0
        },
        {
            "name": "Электронная книга Amazon Kindle Paperwhite 11",
            "sku": "KINDLE-PW11-BLK",
            "description": "Читалка с 6.8'' дисплеем E Ink, подсветкой и влагозащитой IPX8.",
            "price": 17990.0
        }
    ],
    { ordered: false }
)
```

</details>


Создать индекс Wildcard Text Index для коллекции с товарами выполнив команду в клиенте `mongosh`

```js
db.goods.createIndex( { "$**": "text" } )
```

Выполнить запрос к коллекции

```js
db.goods.explain("executionStats").find( { $text: { $search: "консоль" } } )
```

При выполнении запроса используется созданный ранее индекс

<details>
  <summary>план выполнения запроса</summary>

```js
{
  explainVersion: '1',
  queryPlanner: {
    namespace: 'test.goods',
    parsedQuery: {
      '$text': {
        '$search': 'консоль',
        '$language': 'english',
        '$caseSensitive': false,
        '$diacriticSensitive': false
      }
    },
    indexFilterSet: false,
    queryHash: 'CF6F4CEE',
    planCacheShapeHash: 'CF6F4CEE',
    planCacheKey: '08852285',
    optimizationTimeMillis: 1,
    maxIndexedOrSolutionsReached: false,
    maxIndexedAndSolutionsReached: false,
    maxScansToExplodeReached: false,
    prunedSimilarIndexes: false,
    winningPlan: {
      isCached: false,
      stage: 'TEXT_MATCH',
      indexPrefix: {},
      indexName: '$**_text',
      parsedTextQuery: {
        terms: [ 'консоль' ],
        negatedTerms: [],
        phrases: [],
        negatedPhrases: []
      },
      textIndexVersion: 3,
      inputStage: {
        stage: 'FETCH',
        inputStage: {
          stage: 'IXSCAN',
          keyPattern: { _fts: 'text', _ftsx: 1 },
          indexName: '$**_text',
          isMultiKey: true,
          isUnique: false,
          isSparse: false,
          isPartial: false,
          indexVersion: 2,
          direction: 'backward',
          indexBounds: {}
        }
      }
    },
    rejectedPlans: []
  },
  executionStats: {
    executionSuccess: true,
    nReturned: 1,
    executionTimeMillis: 2,
    totalKeysExamined: 1,
    totalDocsExamined: 1,
    executionStages: {
      isCached: false,
      stage: 'TEXT_MATCH',
      nReturned: 1,
      executionTimeMillisEstimate: 0,
      works: 2,
      advanced: 1,
      needTime: 0,
      needYield: 0,
      saveState: 0,
      restoreState: 0,
      isEOF: 1,
      indexPrefix: {},
      indexName: '$**_text',
      parsedTextQuery: {
        terms: [ 'консоль' ],
        negatedTerms: [],
        phrases: [],
        negatedPhrases: []
      },
      textIndexVersion: 3,
      docsRejected: 0,
      inputStage: {
        stage: 'FETCH',
        nReturned: 1,
        executionTimeMillisEstimate: 0,
        works: 2,
        advanced: 1,
        needTime: 0,
        needYield: 0,
        saveState: 0,
        restoreState: 0,
        isEOF: 1,
        docsExamined: 1,
        alreadyHasObj: 0,
        inputStage: {
          stage: 'IXSCAN',
          nReturned: 1,
          executionTimeMillisEstimate: 0,
          works: 2,
          advanced: 1,
          needTime: 0,
          needYield: 0,
          saveState: 0,
          restoreState: 0,
          isEOF: 1,
          keyPattern: { _fts: 'text', _ftsx: 1 },
          indexName: '$**_text',
          isMultiKey: true,
          isUnique: false,
          isSparse: false,
          isPartial: false,
          indexVersion: 2,
          direction: 'backward',
          indexBounds: {},
          keysExamined: 1,
          seeks: 1,
          dupsTested: 1,
          dupsDropped: 0
        }
      }
    }
  },
  queryShapeHash: 'B162AE477B070CE281359D039762D9D7FCBE94D37941723F7DC9BE21457056E2',
  command: {
    find: 'goods',
    filter: { '$text': { '$search': 'консоль' } },
    '$db': 'test'
  },
  serverInfo: {
    host: '3c6c2b1f0f44',
    port: 27017,
    version: '8.0.14',
    gitVersion: 'bbdb887c2ac94424af0ee8fcaad39203bdf98671'
  },
  serverParameters: {
    internalQueryFacetBufferSizeBytes: 104857600,
    internalQueryFacetMaxOutputDocSizeBytes: 104857600,
    internalLookupStageIntermediateDocumentMaxSizeBytes: 104857600,
    internalDocumentSourceGroupMaxMemoryBytes: 104857600,
    internalQueryMaxBlockingSortMemoryUsageBytes: 104857600,
    internalQueryProhibitBlockingMergeOnMongoS: 0,
    internalQueryMaxAddToSetBytes: 104857600,
    internalDocumentSourceSetWindowFieldsMaxMemoryBytes: 104857600,
    internalQueryFrameworkControl: 'trySbeRestricted',
    internalQueryPlannerIgnoreIndexWithCollationForRegex: 1
  },
  ok: 1
}
```
</details>

Поиск успешно выполняется по любому текстовому атрибуту документа. Например документ 

```json
{
    "name": "Игровая консоль Sony PlayStation 5 Slim",
    "sku": "PS5-SLIM-DISC",
    "description": "Консоль нового поколения с дисководом, поддержкой 4K и 1 ТБ SSD.",
    "price": 65990.0
}
```

будет найден любым из следующих запросов

```js
db.goods.find( { $text: { $search: "PlayStation" } } ) // атрибут name
db.goods.find( { $text: { $search: "PS5" } } )  // атрибут sku
db.goods.find( { $text: { $search: "SSD" } } ) // атрибут description
```

> Остановка docker контейнера описана в домашнем задании №1
