import pandas as pd
import time
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Custom pipeline components
from ingest import ingest
from parse import parse_address
from match import exact_match, fuzzy_match
from fallback import phonetic_match, api_match, embedding_match

# Load environment variables from .env
load_dotenv()

# Use DB connection string from environment variable
DB_URL = os.getenv("DB_URL")

def parse_all(engine):
    """
    Reads raw transaction addresses from the database,
    parses them into normalized components using `usaddress`,
    and writes parsed results into `parsed_transactions` table.
    """
    conn = engine.connect()
    result = conn.execute(text(
        "SELECT id, coalesce(address_line_1, '') || ' ' || coalesce(address_line_2, '') AS full_addr, "
        "city, state, zip_code "
        "FROM raw_transactions"
    ))

    rows = result.mappings().all()
    parsed = []

    # Parse each full address using the custom parse_address function
    for row in rows:
        p = parse_address(row['id'], row['full_addr'])
        p['city'] = row['city']
        p['state'] = row['state']
        p['zip_code'] = row['zip_code']
        if p:
            parsed.append(p)

    # Write parsed results to DB
    pd.DataFrame(parsed).to_sql(
        'parsed_transactions',
        engine,
        if_exists='replace',
        index=False
    )

def match_all(engine):
    """
    Attempts to match each parsed transaction record to a canonical address using
    a prioritized waterfall strategy (exact → fuzzy → phonetic → embedding → API).
    Writes both matched and unmatched results to the database and output files.
    """
    conn = engine.connect()
    result = conn.execute(text("SELECT * FROM parsed_transactions"))
    parsed_rows = result.mappings().all()

    results = []
    for row in parsed_rows:
        tx_id = row['id']
        hhid, score, mtype, reason = None, 0.0, None, ''

        # 1. Exact match
        hhid, score = exact_match(conn, row)
        if hhid:
            mtype = 'exact'
            reason = 'exact match'
        else:
            reason = 'no exact match'

            # 2. Fuzzy match
            hhid, score = fuzzy_match(conn, row)
            if hhid:
                mtype = 'fuzzy'
                reason = 'fuzzy match'
            else:
                reason = 'low fuzzy score'

                # 3. Phonetic match
                hhid, score = phonetic_match(conn, row)
                if hhid:
                    mtype = 'phonetic'
                    reason = 'phonetic match'
                else:
                    reason = 'no phonetic match'

                    # 4. Embedding match
                    hhid, score = embedding_match(conn, row, emb_threshold=0.75)
                    if hhid:
                        mtype = 'embedding'
                        reason = 'embedding match'
                    else:
                        reason = 'low embedding score'

                        # 5. API match
                        hhid, score, api_reason = api_match(conn, row)
                        if hhid:
                            mtype = 'api'
                            reason = api_reason
                        else:
                            mtype = 'unmatched'
                            reason = api_reason

        # Retrieve matched canonical address from DB
        matched_address = ''
        if hhid:
            address_row = conn.execute(
                text("SELECT address FROM raw_addresses WHERE hhid = :hhid"),
                {'hhid': hhid}
            ).mappings().first()
            if address_row:
                matched_address = address_row['address']

        # Append result record
        results.append({
            'transaction_id': tx_id,
            'address_id': hhid,
            'matched_address': matched_address,
            'confidence': score,
            'match_type': mtype,
            'reason': reason
        })

    # Save match results to DB and export output files
    df_res = pd.DataFrame(results)
    df_res.to_sql(
        'matches',
        engine,
        if_exists='replace',
        index=False
    )
    df_res.to_csv('matched_output.csv', index=False)
    df_res[df_res['match_type'] == 'unmatched'] \
        .to_json('unmatched_report.json', orient='records', indent=2)

# Entry point to run the full pipeline end-to-end
if __name__ == '__main__':
    engine = create_engine(DB_URL)
    start = time.time()

    ingest(engine)
    parse_all(engine)
    match_all(engine)

    print(f"Pipeline complete in {time.time() - start:.2f}s")
