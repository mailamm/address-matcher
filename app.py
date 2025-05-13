import os
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import matching pipeline components
from parse import parse_address
from match import exact_match, fuzzy_match
from fallback import phonetic_match, api_match, embedding_match

# Create DB engine from environment variable
DB_URL = os.getenv("ADDRESS_DB_URL")
engine = create_engine(DB_URL)

# Initialize Flask app
app = Flask(__name__)

@app.route("/match_address", methods=["POST"])
def match_address():
    """
    REST endpoint to match a single raw address.
    Accepts JSON with field "raw_address".
    Returns a JSON object with match results and metadata.
    """
    payload = request.get_json(force=True)
    raw = payload.get("raw_address", "").strip()

    # Validate required input
    if not raw:
        return jsonify({"error": "raw_address is required"}), 400

    # Generate a mock transaction ID for logging/debugging
    tx_id = raw.lower().replace(" ", "_")

    # 1) Parse the raw address
    parsed = parse_address(tx_id, raw)
    if not parsed:
        return jsonify({"error": "could not parse address"}), 400

    # 2) Run the matching pipeline using waterfall strategy
    with engine.connect() as conn:
        hhid, score, mtype, reason = None, 0.0, None, ""

        # Exact match
        hhid, score = exact_match(conn, parsed)
        if hhid:
            mtype, reason = "exact", "exact match"
        else:
            reason = "no exact match"

            # Fuzzy match
            hhid, score = fuzzy_match(conn, parsed)
            if hhid:
                mtype, reason = "fuzzy", "fuzzy match"
            else:
                reason = "low fuzzy score"

                # Phonetic match
                hhid, score = phonetic_match(conn, parsed)
                if hhid:
                    mtype, reason = "phonetic", "phonetic match"
                else:
                    reason = "no phonetic match"

                    # Embedding match
                    hhid, score = embedding_match(conn, parsed, emb_threshold=0.75)
                    if hhid:
                        mtype, reason = "embedding", "embedding match"
                    else:
                        reason = "low embedding score"

                        # API match
                        hhid, score, api_reason = api_match(conn, parsed)
                        if hhid:
                            mtype, reason = "api", api_reason
                        else:
                            mtype, reason = "unmatched", api_reason or "unmatched"

        # 3) Retrieve canonical matched address (if any)
        matched_addr = None
        if hhid:
            row = conn.execute(
                text("SELECT address FROM raw_addresses WHERE hhid = :hhid"),
                {"hhid": hhid}
            ).mappings().first()
            if row:
                matched_addr = row["address"]

    # Return match results as JSON
    return jsonify({
        "transaction_id":   tx_id,
        "raw_address":      raw,
        "address_id":       hhid,
        "matched_address":  matched_addr,
        "confidence":       round(score, 2),
        "match_type":       mtype,
        "reason":           reason
    })


# Run the Flask app
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
