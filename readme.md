# 📈 PySpark JSON Payload Analyzer Utility

## ✨ Introduction

Developed a high-performance **PySpark** utility to ingest and analyze millions of JSON records in seconds, summarizing crucial structural and statistical insights. This tool is designed to provide rapid data profiling for unstructured or semi-structured JSON payloads, addressing common challenges in modern data engineering pipelines.

<br>

---

## 🚀 Key Contributions & Technical Deep Dive

### **1. Intelligent Data Ingestion & Schema Discovery**
This utility targets serialized **JSON** messages from sources like **Kafka** or cloud storage (**Datalake/File System/BLOB Storage**). While traditional modeling requires a sample to define a rigid schema, this tool provides a proactive, automated approach. It analyzes incoming messages to dynamically discover and summarize:

* **Comprehensive Schema/JSON Structure:** A flattened representation of all keys, including nested paths (e.g., `specifications.color`).
* **Key-Level Frequency Analysis:** Crucial for identifying mandatory fields (`not-null` constraints) and potential primary key combinations.
* **Data Type & Value Profiling:**
    * **Maximum String Length:** Provides column size recommendations for string-based keys.
    * **Highest Value:** Identifies the maximum numerical value for integer and float keys, essential for sizing target table columns.

---

### **2. Optimized Processing with Vectorized UDFs (Pandas UDF)**
To handle complex, row-level JSON parsing logic with optimal performance, a **Vectorized UDF** (also known as a **Pandas UDF**) was implemented.

* **Vectorization for Efficiency:** Unlike a traditional UDF that processes one row at a time, the Pandas UDF operates on batches of data via an in-memory **Apache Arrow** format.
* **Accelerated by Apache Arrow**: The use of this columnar memory format is what enables the high-speed data transfer between JVM and Python workers.
* **Reduced Serialization Overhead:** This approach minimizes the expensive context switching between the PySpark JVM and Python processes, leading to a significant performance boost for CPU-intensive tasks like JSON parsing and schema flattening.

---

### **3. Strategic Data Transformations**
The pipeline orchestrates a series of transformations to efficiently aggregate and profile the data.

* **Array Explosion:** The list of tuples returned by the UDF for each record is efficiently flattened using `pyspark.sql.functions.explode`, transforming the semi-structured data into a row-per-key format.
* **Optimal Aggregation with Explicit Repartitioning:** To ensure all records for a given key are processed on the same machine, the DataFrame is explicitly **repartitioned** by `key_path` before the final aggregation. This prevents redundant data shuffling and optimizes the performance of the subsequent `groupBy` operation, which computes the `max` value and `sum` of frequencies.

---

### **4. Final Output & Insights**
The utility generates a final summary table that can be used directly for data modeling, quality checks, and target system design.

#### Screenshots

**Raw JSON Messages (payload.json)**

![raw payload](/assets/payload_jsons.JPG)

**Final Output of the Transformations**

![raw payload](/assets/final_output.JPG)

The final output columns provide the following key metrics:

* `max_value_or_len`: This column represents the highest numeric value for each key or the maximum length of a string value. For keys that represent a list or map, this value is set to `-1`.
* `key_freq`: This column measures the frequency of each key's occurrence across all processed messages. For array objects, it additionally stores the total sum of element counts, providing a more granular insight into nested data.

### 🧑‍💻 Developed By:

<b>DARSHAN KUMAR</b><br>
 Software Engineer<br>
linkedin: [connect with me, here!](https://www.linkedin.com/in/darshan-k-489226201/)

<br><br><br><br>
---
---