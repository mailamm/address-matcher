# Address Matching Pipeline

This project is a complete end-to-end system for matching messy transaction addresses to canonical addresses using a multi-stage fallback strategy: **exact**, **fuzzy**, **phonetic**, **embedding**, and **external API** (Geocodio).

---

## Setup Instructions (Local)

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/address-matcher.git
cd address-matcher
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Setup PostgreSQL

Ensure PostgreSQL is running locally.


### 4. Create `.env` File

```bash
touch .env
```

Then add PostgreSQL connection string and Geocodio API Key:

```ini
ADDRESS_DB_URL=your_postgres_connection_string
GEOCODIO_API_KEY=your_geocodio_api_key
```

To obtain a Geocodio API key, sign up at [https://www.geocod.io](https://www.geocod.io) and generate a free API key (2,500 daily lookups).

---

## Running the Pipeline (End-to-End)

### Run the full pipeline (ingest → parse → match → export):

```bash
python main.py
```

### Run the REST API for single address matching:

```bash
python app.py
```

### Check the accuracy dashboard (Streamlit):

```bash
streamlit run dashboard.py
```

### Run performance/scaling test:

```bash
python performance.py
```

---

## Estimated Performance at Scale (200M Rows)

| Metric       | Value                                        |
|--------------|----------------------------------------------|
| Dataset      | 200,000,000 rows |           |
| Parse Time   | ~19,536s (~5.4 hours) (extrapolated)                        |
| Match Time   | ~2,188s (~36.5 minutes) (extrapolated)                     |
| Memory ∆     | ~101.98 GB (extrapolated)                    |
| API Rate     | 10% of rows      |
| API Cost     | ~$10,000 for 200M rows (at $0.50 per 1,000) |

**Notes on Performance Estimates:**

The statistics above were extrapolated based on a local run using a scale factor of 100 (32,100 rows). Runtime and memory usage were projected linearly.
The estimated API cost assumes approximately 10% of records fall back to API matching due to missing or unmatched canonical addresses. This percentage may vary depending on the completeness and quality of the canonical address dataset provided.

---


## Design Overview

### Libraries Used

- **pandas** – data manipulation  
- **SQLAlchemy** – database access  
- **psycopg2-binary** – PostgreSQL driver  
- **python-dotenv** – loading environment variables  
- **usaddress** – parsing US address components  
- **RapidFuzz** – fast fuzzy string matching  
- **Metaphone** – phonetic similarity
- **sentence-transformers** – address embedding
- **scikit-learn** – cosine similarity and metrics  
- **Flask** – REST API framework  
- **Streamlit** – dashboard and visualization  
- **Geocodio API** – real-world geocoding fallback  

### Matching Strategy

* **Exact Match** – match by fully normalized address fields using a database join
* **Fuzzy Match** – match by token sort ratio on the full street name
* **Phonetic Match** – match by pronunciation similarity using phonetic encoding
* **Embedding Match** – match by cosine similarity of dense semantic embeddings
* **API Match** – match by normalized components returned from the Geocodio API fallback

### Blocking Strategies

* Block by **house number** to reduce candidate pool
* Filter by **first letter** or **Metaphone code** for fuzzy/phonetic steps

### Assumptions

* All data is located in Brooklyn, NY 11211, so comparisons on ZIP code, city, and state were omitted to reduce processing time. 
* Transaction addresses may be messy, abbreviated, or incomplete
* Ground-truth was duplicated from `matched_output.csv` for testing purposes
* Matching is performed only when the apartment/unit number exactly matches between the transaction and canonical address.

### Limitations

* The free tier of Geocodio allows only 2,500 requests/day, which limits fallback usage at scale unless upgraded to a paid plan.
* Fuzzy/phonetic thresholds are tuned around 70 to balance precision. Some edge cases may slip through but are later caught by API fallback
* Embedding model may not be useful in this project due to limited data
* Matching runs in a Python loop, which is slow for large datasets. SQL-based batch matching is in progress to improve speed.
* Containerization is in progress to streamline setup and ensure consistent environments.

---

## Input and Output Data

| File                        | Purpose                             |
| --------------------------- | ----------------------------------- |
| `transactions_2_11211.xlsx` | Messy transaction addresses (input) |
| `11211 Addresses.xlsx`      | Canonical address reference (truth) |
| `matched_output.csv`        | Output from full address matching pipeline     |
|`ground_truth.csv`        |  Created by duplicating `matched_output.csv` to simulate labeled pairs for dashboard evaluation |

---

## File Descriptions

| File/Folder        | Description                                                               |
| ------------------ | ------------------------------------------------------------------------- |
| `main.py`          | Runs the full pipeline: ingest → parse → match → export                   |
| `ingest.py`        | Reads Excel files and loads data into raw staging tables                  |
| `parse.py`         | Uses `usaddress` to extract address components                            |
| `match.py`         | Implements exact and fuzzy match logic using SQL and RapidFuzz            |
| `fallback.py`      | Handles phonetic, embedding, and API-based fallback matching strategies   |
| `app.py`           | Flask API for single-address match requests via `/match_address` endpoint |
| `dashboard.py`     | Streamlit dashboard to visualize match accuracy using ground truth        |
| `performance.py`   | Performance and cost testing using duplicated datasets                    |
| `schema.sql`       | SQL schema to create required PostgreSQL tables                           |
| `requirements.txt` | Python package dependencies                                               |
| `.env`             | Stores secrets like database URL and API keys |

---
