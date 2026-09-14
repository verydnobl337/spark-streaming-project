# Spark Streaming Project

## Description

Streaming-пайплайн на PySpark Structured Streaming, который читает ресторанные рекламные события из Kafka, сопоставляет их с подписками пользователей в PostgreSQL и формирует персонализированные триггеры. Обработанные события записываются в PostgreSQL для обратной связи и одновременно публикуются в выходной Kafka topic.

**Tags:** `PySpark` `Spark Structured Streaming` `Apache Kafka` `PostgreSQL` `JDBC` `Python` `Streaming ETL` `Real-time Data Processing`

## Архитектура

```text
                         ┌─────────────────────────┐
                         │       Apache Kafka      │
                         │   Input topic / events  │
                         └────────────┬────────────┘
                                      │
                                      │ JSON events
                                      ▼
                         ┌─────────────────────────┐
                         │   PySpark Structured     │
                         │       Streaming         │
                         │                         │
                         │  1. Read Kafka stream   │
                         │  2. Parse JSON          │
                         │  3. Filter active ads   │
                         │  4. Join subscriptions  │
                         └────────────┬────────────┘
                                      │
                         ┌────────────┴────────────┐
                         │                         │
                         ▼                         ▼
              ┌─────────────────────┐   ┌─────────────────────┐
              │     PostgreSQL      │   │       Kafka          │
              │ feedback / target   │   │   Output topic      │
              │                     │   │                     │
              │ subscribers_feedback│   │ personalized events │
              └─────────────────────┘   └─────────────────────┘

                         ▲
                         │ JDBC
                         │
              ┌─────────────────────┐
              │     PostgreSQL      │
              │      source         │
              │                     │
              │ subscribers_        │
              │ restaurants         │
              └─────────────────────┘
```

## Data Flow

1. Spark Structured Streaming подключается к входному Kafka topic.
2. Сообщения Kafka читаются как JSON и десериализуются по заданной схеме.
3. Из потока отбираются только рекламные кампании, активные в текущий момент.
4. Из PostgreSQL через JDBC загружается таблица `subscribers_restaurants`.
5. Поток событий объединяется с подписками по `restaurant_id`.
6. Для каждого подходящего события добавляется `client_id` и время формирования триггера.
7. Через `foreachBatch` результат записывается в PostgreSQL.
8. Из результата удаляется поле `feedback`, данные сериализуются обратно в JSON и публикуются в выходной Kafka topic.

## Структура проекта

```text
spark-streaming-project/
├── project.py            # основной streaming pipeline
├── config.py             # конфигурация из переменных окружения
├── kafka-read.py         # тест чтения и десериализации Kafka stream
├── postgres-read.py      # тест чтения данных из PostgreSQL
├── join-test.py          # тест Kafka + PostgreSQL join и фильтрации
├── .env.example          # пример переменных окружения без секретов
├── .gitignore
└── README.md
```

## Назначение скриптов

### `project.py`

Основной production-like pipeline:

`Kafka → PySpark → filtering → PostgreSQL subscriptions → join → PostgreSQL + Kafka`

Используется `foreachBatch`, чтобы один micro-batch отправлять одновременно в PostgreSQL и выходной Kafka topic.

### `kafka-read.py`

Изолированный тест Kafka-интеграции. Читает входной topic, десериализует JSON и выводит преобразованные события в консоль.

### `postgres-read.py`

Изолированный тест JDBC-интеграции с PostgreSQL. Читает таблицу подписок и выводит данные в консоль.

### `join-test.py`

Интеграционный тест основной логики без записи в конечные системы. Объединяет Kafka stream с таблицей подписок PostgreSQL и выводит результат в консоль.

### `config.py`

Единая точка конфигурации. Все параметры подключения к Kafka и PostgreSQL, credentials, topics и таблицы передаются через environment variables.

## Конфигурация

Скрипты **не содержат credentials, адресов инфраструктуры или названий конкретных Kafka topics**. Перед запуском необходимо создать переменные окружения на основе `.env.example`.

Пример:

```bash
set -a
source .env
set +a
```

Для Windows PowerShell переменные можно задать так:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "your-kafka-host:9091"
$env:KAFKA_USERNAME = "your-kafka-username"
$env:KAFKA_PASSWORD = "your-kafka-password"
$env:KAFKA_INPUT_TOPIC = "your-input-topic"
$env:KAFKA_OUTPUT_TOPIC = "your-output-topic"
```

Полный список переменных находится в `.env.example`.

> `.env` с реальными credentials не должен добавляться в Git. Он уже исключён через `.gitignore`.

## Технологический стек

- Python
- Apache Spark 3.3.0
- PySpark Structured Streaming
- Apache Kafka
- PostgreSQL
- JDBC
- JSON

## Запуск

Для запуска нужен установленный Apache Spark с поддержкой PySpark и доступ к Kafka/PostgreSQL.

Основной pipeline:

```bash
spark-submit project.py
```

Тест Kafka:

```bash
spark-submit kafka-read.py
```

Тест PostgreSQL:

```bash
spark-submit postgres-read.py
```

Тест объединения Kafka и PostgreSQL:

```bash
spark-submit join-test.py
```

Зависимости Spark для Kafka и PostgreSQL подключаются непосредственно через `spark.jars.packages`.

## Безопасность

Секреты и параметры инфраструктуры вынесены из исходного кода в environment variables. Не коммитьте `.env`, реальные passwords, SASL credentials или приватные connection strings в репозиторий.
