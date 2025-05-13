import pandas as pd
import time
import psutil
import os
from sqlalchemy import create_engine
from parse import parse_address
from fallback import api_match as real_api_match
import fallback
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Override real API call with stub for main matching logic
# This ensures scalability testing doesn't hit rate-limited APIs
fallback.api_match = lambda conn, p: (None, 0.0, "stubbed")

# Import pipeline functions
from main import parse_all, match_all

# Cost per 1,000 API calls (used for cost estimation)
RATE_PER_1K = 0.5

# Get DB connection string from environment variable
DB_URL = os.getenv("DB_URL")

# Path to base transaction file (used for duplication)
BASE_FILE = 'transactions_2_11211.xlsx'

def scale_test(factor: int):
    """
    Simulate large-scale matching workload by:
    - Duplicating base dataset `factor` times
    - Measuring time/memory usage for parsing and matching
    - Estimating extrapolated cost for full 200M-row scale
    - Sampling API usage separately on 1K rows
    """
    engine = create_engine(DB_URL)
    with engine.connect() as conn:

        # 1) Build large dataset by duplicating base rows
        df = pd.read_excel(BASE_FILE)
        df_large = pd.concat([df] * factor, ignore_index=True)
        df_large.to_sql('raw_transactions', conn, if_exists='replace', index=False)

        # 2) Record memory usage before parse/match
        proc = psutil.Process()
        mem_before = proc.memory_info().rss

        # 3) Time parsing step
        t0 = time.time()
        parse_all(engine)
        parse_time = time.time() - t0

        # 4) Time matching step
        t1 = time.time()
        match_all(engine)
        match_time = time.time() - t1

        # Measure memory delta
        mem_after = proc.memory_info().rss

        # 5) Extrapolate timings and memory to 200 million rows
        base_rows = len(df)
        actual_rows = base_rows * factor
        parse_per_row = parse_time / actual_rows
        match_per_row = match_time / actual_rows
        mem_per_row = (mem_after - mem_before) / actual_rows

        print(f"Scale factor: {factor}× ({actual_rows} rows)")
        print(
            "Extrapolated 200M rows → "
            f"Parse: {parse_per_row * 200_000_000:.0f}s, "
            f"Match: {match_per_row * 200_000_000:.0f}s"
        )
        total_mem_gb = mem_per_row * 200_000_000 / 1e9
        print(f"Extrapolated Memory Δ: {total_mem_gb:.2f} GB")

        # 6) Test real API matching on 1,000-row sample for rate/cost estimation
        sample = df.sample(1000, replace=True)
        start = time.time()
        for _, row in sample.iterrows():
            raw = f"{row['address_line_1']} {row['address_line_2']}".strip()
            parsed = parse_address("sample", raw)
            parsed['city'] = row.get('city', '')
            parsed['state'] = row.get('state', '')
            parsed['zip_code'] = row.get('zip_code', '')
            real_api_match(conn, parsed)
        elapsed = time.time() - start

        # Compute throughput and cost estimate
        rate = 1000 / elapsed
        cost = (200_000_000 / 1000) * RATE_PER_1K
        print(f"API rate: {rate:.1f} calls/sec")
        print(f"Estimated cost for 200M calls: ${cost:,.2f}")

# Run performance test
if __name__ == '__main__':
    scale_test(factor=100)
