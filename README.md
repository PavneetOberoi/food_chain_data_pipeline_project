# Food Chain Data Engineering Pipeline

An end-to-end data engineering project that demonstrates how restaurant and food-chain operational data can be ingested, processed, transformed, and organized into a scalable data platform using **Azure Event Hubs, Databricks, PySpark, SQL, and Delta Lake**.

The project focuses on building a production-oriented data pipeline with **streaming ingestion, structured transformations, configurable pipelines, and medallion-style data organization**.

---

## Architecture

```text
                         ┌─────────────────────┐
                         │   Synthetic Data    │
                         │     Generation      │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │   Restaurant /      │
                         │   Food Chain Data   │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │   Azure Event Hubs  │
                         │   Streaming Layer   │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Azure Databricks    │
                         │ Structured Streaming│
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │    Bronze Layer     │
                         │   Raw / Ingested    │
                         │       Data          │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Silver / Processing │
                         │   Transformations   │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Analytical / SQL    │
                         │      Layer          │
                         └─────────────────────┘
```

The streaming ingestion pipeline reads events from Azure Event Hubs through its Kafka-compatible interface and parses the incoming JSON payload into a structured Spark DataFrame.

---

## Key Features

### 1. Event-driven ingestion

The project uses **Azure Event Hubs** as the streaming ingestion layer.

Event Hubs is accessed through the Kafka protocol, allowing the Spark streaming application to consume events using Kafka-compatible configuration.

The ingestion pipeline:

* Connects to an Azure Event Hub
* Consumes events using Spark Structured Streaming
* Reads events from Kafka-compatible endpoints
* Parses the incoming message payload
* Applies an explicit Spark schema
* Produces a structured Bronze table

The pipeline is configured with controls such as:

* `maxOffsetsPerTrigger`
* `startingOffsets`
* `failOnDataLoss`
* Kafka security configuration
* Session/request timeouts

This provides explicit control over how streaming data is consumed.

---

## 2. Structured Streaming with PySpark

The ingestion pipeline uses **PySpark Structured Streaming**.

Incoming Kafka/Event Hubs messages are initially represented as binary key/value data. The pipeline converts the message value to a string and parses it using `from_json()` with an explicit `StructType` schema.

Example flow:

```text
Event Hubs
    ↓
Kafka-compatible stream
    ↓
Spark readStream
    ↓
Binary key/value
    ↓
String conversion
    ↓
JSON parsing
    ↓
Explicit Spark schema
    ↓
Structured DataFrame
```

The order schema contains fields such as:

* `order_id`
* `timestamp`
* `restaurant_id`
* `customer_id`
* `order_type`
* `items`
* `total_amount`
* `payment_method`
* `order_status`

The resulting timestamp column is also renamed to `order_timestamp` for clarity.

---

## 3. Medallion-style Data Architecture

The project follows a layered data architecture.

### Bronze

The Bronze layer represents the ingestion layer and retains structured data coming from the streaming source.

The streaming table is explicitly configured with the property:

```text
quality = bronze
```

This provides a clear separation between ingestion and downstream processing.

### Silver

The Silver layer is intended for cleaned, validated, and transformed operational data.

Typical transformations in this layer can include:

* Data type normalization
* Data cleansing
* Validation
* Deduplication
* Business transformations
* Preparing data for analytical workloads

### Analytical Layer

The final layer contains SQL-based transformations and structures intended to support analytical use cases.

---

## 4. Configurable Pipeline Design

Pipeline configuration is separated from the processing logic.

The repository contains a `pipeline_configuration.json` file that defines pipeline-level configuration including:

* Pipeline name
* Catalog
* Schema
* Gateway connection
* Gateway storage location
* Cluster configuration
* Continuous execution settings

For example:

```json
{
    "name": "gw_ingestion_silver",
    "catalog": "ws_dbxproject",
    "schema": "00_landing",
    "continuous": true
}
```

This separation makes the pipeline configuration easier to modify without changing the core processing code.

---

## Technology Stack

| Technology                     | Purpose                                       |
| ------------------------------ | --------------------------------------------- |
| **Python**                     | Pipeline and data engineering logic           |
| **PySpark**                    | Distributed data processing                   |
| **Spark Structured Streaming** | Streaming ingestion                           |
| **Azure Event Hubs**           | Event streaming / ingestion                   |
| **Kafka Protocol**             | Event Hubs integration with Spark             |
| **Azure Databricks**           | Data processing platform                      |
| **Delta Lake**                 | Lakehouse storage                             |
| **SQL**                        | Data transformation and analytical processing |
| **JSON**                       | Pipeline and event configuration              |

---

## Repository Structure

```text
food_chain_data_pipeline_project/
│
├── data_generation_script/
│   └── Synthetic data generation scripts
│
├── pipelines/
│   └── Data processing and transformation pipelines
│
├── sql/
│   └── SQL scripts for data processing / analytical workloads
│
├── synthetic_data/
│   └── Generated food-chain datasets
│
├── pipeline_ingest_eventhub.py
│   └── Event Hubs → Spark Structured Streaming ingestion
│
├── pipeline_configuration.json
│   └── Pipeline configuration
│
├── requirements.txt
│   └── Python dependencies
│
└── .gitignore
```

---

## Data Flow

The main streaming ingestion flow is:

### Step 1 — Generate operational data

Synthetic restaurant/food-chain data is generated for the project.

### Step 2 — Publish events

The generated events can be published to Azure Event Hubs.

### Step 3 — Consume events

Azure Databricks consumes the Event Hubs stream through its Kafka-compatible interface.

### Step 4 — Parse events

The raw event payload is converted from Kafka bytes into strings and parsed into a structured Spark schema.

### Step 5 — Bronze ingestion

The structured streaming data is written to the Bronze layer.

### Step 6 — Transform

Downstream pipelines perform cleansing and transformation to prepare the data for analytical workloads.

### Step 7 — SQL processing

SQL transformations can then be used to create analytical datasets and business-facing outputs.

---

## Example: Event Hubs → Databricks

The project configures Spark to communicate with Event Hubs using Kafka-compatible settings:

```python
KAFKA_OPTIONS = {
    "kafka.bootstrap.servers":
        f"{EH_NAMESPACE}.servicebus.windows.net:9093",

    "subscribe": EH_NAME,

    "kafka.sasl.mechanism": "PLAIN",

    "kafka.security.protocol": "SASL_SSL",

    "maxOffsetsPerTrigger": "50000",

    "failOnDataLoss": "true",

    "startingOffsets": "earliest"
}
```

The stream is then consumed using:

```python
spark.readStream \
    .format("kafka") \
    .options(**KAFKA_OPTIONS) \
    .load()
```

The incoming JSON is parsed using an explicit Spark schema:

```python
.withColumn(
    "data",
    from_json("value_str", orders_schema)
)
```

This approach avoids relying on schema inference for the streaming payload and gives the pipeline a defined contract for incoming order data.

---

## Configuration

Before running the pipeline, configure the required Event Hubs connection parameters through the Databricks/Spark configuration:

```text
eh.namespace
eh.name
eh.connectionString
```

The application retrieves these values using Spark configuration rather than hard-coding them directly into the pipeline:

```python
EH_NAMESPACE = spark.conf.get("eh.namespace")
EH_NAME = spark.conf.get("eh.name")
EH_CONN_STR = spark.conf.get("eh.connectionString")
```

For production deployments, credentials should be stored using a secure secret-management mechanism rather than committed to source control.

---

## Running the Project

### 1. Clone the repository

```bash
git clone https://github.com/PavneetOberoi/food_chain_data_pipeline_project.git

cd food_chain_data_pipeline_project
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Generate synthetic data

Use the scripts under:

```text
data_generation_script/
```

to generate the required project data.

### 4. Configure Azure resources

Set up:

* Azure Event Hubs
* Azure Databricks
* Required storage
* Required catalog/schema
* Event Hubs connection configuration

### 5. Configure the pipeline

Update:

```text
pipeline_configuration.json
```

with the appropriate environment-specific configuration.

### 6. Run the ingestion pipeline

Deploy/run:

```text
pipeline_ingest_eventhub.py
```

using the Databricks environment.

### 7. Run downstream transformations

Execute the pipelines and SQL scripts under:

```text
pipelines/
sql/
```

to process the ingested data.

---

## Engineering Concepts Demonstrated

This project demonstrates several practical data engineering concepts:

* Batch and streaming data processing
* Event-driven architecture
* Azure Event Hubs
* Kafka-compatible ingestion
* Spark Structured Streaming
* Explicit data schemas
* JSON parsing
* PySpark transformations
* Medallion architecture
* Lakehouse architecture
* Configuration-driven pipelines
* SQL transformations
* Synthetic data generation
* Distributed data processing

---

## Design Considerations

### Explicit schema

The streaming pipeline uses an explicit `StructType` rather than attempting to infer the structure of every incoming event.

This makes the expected data contract clear and helps downstream processing remain predictable.

### Controlled streaming ingestion

The pipeline defines `maxOffsetsPerTrigger`, allowing the amount of data processed in a micro-batch to be controlled.

This can help manage processing load as event volume increases.

### Separation of configuration and logic

Pipeline configuration is maintained separately from transformation code, reducing the need to modify application logic for environment-specific settings.

### Scalable processing

Spark Structured Streaming provides distributed processing capabilities, allowing the architecture to scale beyond a single-process ingestion application.

---

## Future Improvements

Potential extensions to the project include:

* Add automated data-quality checks
* Implement explicit Bronze → Silver → Gold transformations
* Add checkpointing and recovery configuration
* Add dead-letter/quarantine handling for malformed events
* Implement schema evolution strategies
* Add data lineage and observability
* Add automated testing
* Add CI/CD using GitHub Actions
* Add infrastructure-as-code using Terraform
* Add monitoring and alerting
* Add incremental processing and deduplication
* Add Databricks Unity Catalog governance
* Add production-grade secret management

---

## Learning Objectives

The project was built to explore how a modern data platform can combine:

**Event Streaming + Distributed Processing + Lakehouse Storage + SQL + Cloud Infrastructure**

The primary focus is on understanding the engineering decisions required to move from raw operational events to structured, analytical data in a scalable architecture.

---

## Author

**Pavneet Oberoi**

Data Engineer | Python
