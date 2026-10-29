# Procesamiento de Datos con Apache Spark y Kafka - Tarea 3 (Big Data UNAD)

**Autor:** Andriy David Sanguino Martínez  
**Curso:** Big Data (202016911) - Universidad Nacional Abierta y a Distancia (UNAD)

## 1. Descripción de la Solución
Este repositorio contiene la implementación práctica de procesamiento de datos masivos utilizando Python, Apache Spark (PySpark) y Apache Kafka, dividida en dos componentes:

1. **Procesamiento en Batch (`analisis_batch.py`):** 
   - Utiliza conjuntos de datos públicos del portal *Datos Abiertos Colombia* (`datos.gov.co`) enfocados en seguridad ciudadana y registros delictivos.
   - Realiza la carga distribuida mediante DataFrames de Spark, inferencia de esquema, limpieza de valores nulos (`dropna`) y Análisis Exploratorio de Datos (EDA) agrupando y ordenando las categorías con mayor incidencia.

2. **Procesamiento en Tiempo Real - Anexo 3 (`kafka_producer.py` y `spark_streaming_consumer.py`):**
   - Configura un flujo continuo de eventos utilizando Apache ZooKeeper y Apache Kafka (`topic: sensor_data`).
   - El productor en Python simula eventos de sensores en formato JSON (`sensor_id`, `temperature`, `humidity`, `timestamp`) y los publica en el broker local (`localhost:9092`).
   - El consumidor en Spark Structured Streaming se suscribe al topic, aplica el esquema y calcula promedios en ventanas de tiempo de 1 minuto.

---

## 2. Archivos del Repositorio
- `analisis_batch.py`: Script de PySpark para la carga, limpieza y análisis exploratorio (EDA) en lote.
- `kafka_producer.py`: Script generador de datos en tiempo real hacia el topic de Kafka.
- `spark_streaming_consumer.py`: Aplicación de Spark Structured Streaming que consume y agrega los datos en tiempo real.

---

## 3. Instrucciones de Ejecución

### A. Ejecución del Procesamiento en Batch
1. Descargar el conjunto de datos en formato CSV desde Datos Abiertos Colombia:
   ```bash
   wget -O delitos_informaticos.csv "[https://www.datos.gov.co/api/views/x46e-abhz/rows.csv?accessType=DOWNLOAD](https://www.datos.gov.co/api/views/x46e-abhz/rows.csv?accessType=DOWNLOAD)"
