# ride-sharing-spark-streaming
Spark structured streaming example
# Real-Time Ride-Sharing Analytics with Apache Spark

## **Overview**
In this project, you will build a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming. The goal is to process streaming ride data, compute real-time aggregations, and extract trends using time-based windows.

---

## **Project Structure**
```
RideSharingAnalytics/
├── output/
│   ├── task1/
│   ├── task2/
│   └── task3/
├── data_generator.py
├── task1.py
├── task2.py
├── task3.py
└── README.md
```

---

## **Getting Started**

## **Prerequisites**

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Apache Spark**:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```
  4. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install faker
     ```
   

---

### **2. Dataset Generation**
Run the script to generate synthetic ride-sharing metadata and logs:

```bash
python data_generator.py
```
This will populate the `input/` folder with `songs_metadata.csv` and `listening_logs.csv`. It includes users with biased behavior to simulate genre loyalty.

---

## **Assignment Tasks**

### **Task 1: Ingest and Parse Streaming Data**
**Script:** `task1.py`

- Ingest data from a socket (e.g. localhost:9999)
- Parse incoming JSON data into a Spark DataFrame
- Display parsed data in the console

Run:
```bash
python task1.py
```

---

### **Task 2: Real-Time Aggregations (Driver-Level)**
**Script:** `task2.py`

- Group by `driver_id`
- Compute:
  - Total fare amount (`SUM(fare_amount)`)
  - Average distance (`AVG(distance_km)`)
- Write results to `output/task2/`

Run:
```bash
python task2.py
```

---

### **Task 3: Sliding Window Analysis**
**Script:** `task3.py`

- Convert `timestamp` to proper `TimestampType`
- Use `window()` for a **5-minute sliding window (every 1 minute)**
- Aggregate `fare_amount` in this window
- Write results to `output/task3/`

Run:
```bash
python task3.py
```

---

### **Genre Loyalty Analysis**
**Script:** `task3.py` *(or include within it as needed)*

- Merges listening logs with song metadata
- Calculates each user’s top genre and loyalty score:
  ```
  loyalty_score = plays_in_top_genre / total_plays
  ```
- Outputs users with loyalty score > 0.8

Output (optional): `output/genre_loyalty.csv`

---

## 📦 Outputs
- Parsed streaming input in `output/task1/`
- Driver-level aggregations in `output/task2/`
- Sliding window fare sums in `output/task3/`
- (Optional) Genre loyalty results in `output/genre_loyalty.csv`

---

## 📬 Submission Checklist
- [x] Datasets in `input/`
- [x] All Python scripts in project root
- [x] Generated results in `output/`
- [x] Updated `README.md`
- [x] GitHub repo pushed and submitted
