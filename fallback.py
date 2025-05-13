import os
import requests
from rapidfuzz import fuzz
from metaphone import doublemetaphone
from sqlalchemy import text
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()


def phonetic_match(conn, parsed: dict, threshold: int = 70):
    """
    Perform phonetic matching using Double Metaphone and fuzzy scoring.

    1) Block on street_number
    2) Phonetic‐filter on street name
    3) Filter out any candidate whose unit != parsed unit
    4) If more than one remains, fuzzy‐score parsed_full vs canonical address
    5) Return best match if score ≥ threshold
    """
    num = (parsed.get('street_number') or "").strip().upper()
    parsed_unit = (parsed.get('unit') or "").strip().upper()
    if not num:
        return None, 0.0

    # Query candidates with the same house number
    q = text("""
        SELECT hhid, street, address, aptnbr
          FROM raw_addresses
         WHERE house = :num
    """)
    rows = conn.execute(q, {'num': num}).mappings().all()
    if not rows:
        return None, 0.0

    # Build the full parsed street (for later fuzzy scoring)
    tokens = [
        parsed.get('predir', ''),
        parsed.get('street_name', ''),
        parsed.get('street_type', ''),
        parsed.get('postdir', '')
    ]
    parsed_full = " ".join(t for t in tokens if t).upper()

    # Filter candidates with matching phonetic street names
    target_code = doublemetaphone(parsed.get('street_name', '').upper())[0]
    phonetic = [
        r for r in rows
        if doublemetaphone((r['street'] or '').upper())[0] == target_code
    ]
    if not phonetic:
        return None, 0.0

    # Filter candidates where unit does not match
    if parsed_unit:
        phonetic = [
            r for r in phonetic
            if (r.get('aptnbr') or "").strip().upper() == parsed_unit
        ]
        if not phonetic:
            return None, 0.0

    # Select best match using fuzzy string similarity on full address
    best_hhid, best_score = None, 0.0
    for r in phonetic:
        score = fuzz.token_set_ratio(parsed_full, (r['address'] or "").upper())
        if score > best_score:
            best_hhid, best_score = r['hhid'], score

    if best_score >= threshold:
        return best_hhid, round(best_score, 2)
    return None, 0.0


# Load sentence embedding model once to reuse across function calls
EMB_MODEL = SentenceTransformer('all-MiniLM-L6-v2')


def embedding_match(conn, parsed: dict, emb_threshold: float = 0.75):
    """
    Perform fallback address match using text embeddings and cosine similarity.

    1) Block on house number
    2) Compute embeddings for parsed_full and each candidate.address
    3) Enforce exact unit match
    4) Return best match if cosine similarity ≥ threshold
    """
    tokens = [
        parsed.get('predir', ''),
        parsed.get('street_name', ''),
        parsed.get('street_type', ''),
        parsed.get('postdir', '')
    ]
    parsed_full = ' '.join(t for t in tokens if t).upper()
    num = (parsed.get('street_number') or '').strip().upper()
    parsed_unit = (parsed.get('unit') or '').strip().upper()
    if not num:
        return None, 0.0

    # Retrieve candidates with matching house number
    q = text("SELECT hhid, address, aptnbr FROM raw_addresses WHERE house = :num")
    rows = conn.execute(q, {'num': num}).mappings().all()
    if not rows:
        return None, 0.0

    # Generate embedding for the parsed input
    parsed_emb = EMB_MODEL.encode(parsed_full, convert_to_numpy=True).reshape(1, -1)

    best_hhid, best_score = None, 0.0
    for r in rows:
        # Filter by unit if provided
        canon_unit = (r.get('aptnbr') or '').strip().upper()
        if parsed_unit and parsed_unit != canon_unit:
            continue

        # Generate embedding for the candidate address
        cand_addr = (r['address'] or '').strip().upper()
        cand_emb = EMB_MODEL.encode(cand_addr, convert_to_numpy=True).reshape(1, -1)

        # Compute cosine similarity
        cos_sim = float(cosine_similarity(parsed_emb, cand_emb)[0, 0])
        if cos_sim > best_score:
            best_hhid, best_score = r['hhid'], cos_sim

    if best_hhid and best_score >= emb_threshold:
        return best_hhid, round(best_score * 100, 2)
    return None, 0.0


def api_match(conn, parsed: dict):
    """
    Fallback using external address validation API (e.g. Geocodio).

    1) Build full address string using raw components.
    2) Call API and parse response.
    3) Attempt exact match using API-returned normalized components.
    """
    # Load API key from environment for security
    key = os.getenv("GEOCODIO_API_KEY")
    if not key:
        return None, 0.0, "no api key"

    # Build complete address query string
    street_part = parsed['original_address']
    city_part = parsed.get('city', '')
    state_part = parsed.get('state', '')
    zip_part = parsed.get('zip_code', '')
    full = f"{street_part}, {city_part}, {state_part} {zip_part}".strip()

    if not full:
        return None, 0.0, "no original_address"

    # Call the API with timeout and error handling
    try:
        resp = requests.get(
            "https://api.geocod.io/v1.7/geocode",
            params={"q": full, "api_key": key},
            timeout=5
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"[api_match] request error for '{full}':", e)
        return None, 0.0, "api request error"

    data = resp.json()
    results = data.get("results", [])
    if not results:
        return None, 0.0, "no api result"

    # Parse the top result for normalized address components
    top = results[0].get("address_components", {})
    confidence = results[0].get("accuracy", 0.0)

    house = (top.get("number") or "").strip().upper()
    predir = (top.get("predirectional") or "").strip().upper()
    street = (top.get("street") or "").strip().upper()
    strtype = (top.get("suffix") or "").strip().upper()
    postdir = (top.get("postdirectional") or "").strip().upper()
    apt_type = (top.get("secondaryunit") or "").strip().upper()
    unit = (top.get("secondarynumber") or "").strip().upper()
    zip5 = (top.get("zip") or "").strip()

    # Attempt exact DB match using normalized components from API
    q = text("""
        SELECT hhid FROM raw_addresses
         WHERE house   = :house
           AND street  = :street
           AND zip     = :zip5
           AND (:predir   = '' OR predir   = :predir)
           AND (:postdir  = '' OR postdir  = :postdir)
           AND (:strtype  = '' OR strtype  = :strtype)
           AND (
                (:apt_type = ''     AND apttype = '')
            OR (:apt_type <> ''    AND apttype = :apt_type)
           )
           AND (
                (:unit     = ''     AND aptnbr = '')
            OR (:unit     <> ''    AND aptnbr = :unit)
           )
    """)
    params = {
        "house": house,
        "street": street,
        "zip5": zip5,
        "predir": predir,
        "postdir": postdir,
        "strtype": strtype,
        "apt_type": apt_type,
        "unit": unit
    }
    # print("[api_match] SQL params:", params)
    row = conn.execute(q, params).mappings().first()

    if row:
        return row["hhid"], confidence, "api match"

    return None, 0.0, "api returned but no match"
