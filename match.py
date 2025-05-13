from rapidfuzz import fuzz
from sqlalchemy import text


def exact_match(conn, parsed: dict):
    """
    Attempt exact match of parsed transaction address against canonical addresses.
    Matching is done on street number, directionals, street name/type, and unit info.
    Returns a tuple of (hhid, confidence), where confidence is 1.0 if matched.
    """
    q = text("""
        SELECT hhid
          FROM raw_addresses
         WHERE house   = :street_number
           AND (
                (predir   = :predir)
             OR (predir   = '' AND :predir   = '')
               )
           AND street  = :street_name
           AND strtype = :street_type
           AND (
                (postdir  = :postdir)
             OR (postdir  = '' AND :postdir  = '')
               )
           AND (
                (apttype  = :apt_type AND aptnbr = :unit)
             OR ( :apt_type = ''   AND :unit    = '' 
                  AND apttype = ''   AND aptnbr  = '' )
               )
    """)
    # Execute query with parameters from parsed input
    row = conn.execute(q, parsed).mappings().first()

    # If a match is found, return hhid with confidence score of 1.0
    return (row['hhid'], 1.0) if row else (None, 0.0)


def fuzzy_match(conn, parsed: dict, threshold: float = 70.0):
    """
    Attempt fuzzy match of parsed transaction address against canonical addresses.

    Matching strategy:
    1) Block on street number (to reduce candidate pool).
    2) Fuzzy compare the full street string (e.g. "N Main Street").
    3) Require unit number to match exactly.
    4) Return the best match if the fuzzy score exceeds the threshold.

    Returns (hhid, score) if matched, else (None, 0.0).
    """
    num = parsed.get('street_number', '').strip()
    if not num:
        return None, 0.0  # Can't match without street number

    # Build the parsed full street string (used for fuzzy matching)
    tokens = [
        parsed.get('predir', ''),
        parsed.get('street_name', ''),
        parsed.get('street_type', ''),
        parsed.get('postdir', '')
    ]
    parsed_full = ' '.join(t for t in tokens if t).upper()

    # Query candidates that share the same street number (blocking)
    q = text("""
        SELECT hhid, predir, street, strtype, postdir, apttype, aptnbr
          FROM raw_addresses
         WHERE house = :num
    """)
    candidates = conn.execute(q, {'num': num}).mappings().all()

    # Initialize best match tracker
    best = {'hhid': None, 'score': 0.0, 'apttype': '', 'aptnbr': ''}
    for r in candidates:
        # Construct full canonical street string
        canon_tokens = [
            r.get('predir', ''),
            r['street'],
            r.get('strtype', ''),
            r.get('postdir', '')
        ]
        canon_full = ' '.join(t for t in canon_tokens if t).upper()

        # Compute fuzzy similarity score
        score = fuzz.token_sort_ratio(parsed_full, canon_full)
        if score > best['score']:
            best.update({
                'hhid': r['hhid'],
                'score': score,
                'apttype': r.get('apttype', ''),
                'aptnbr': r.get('aptnbr', '')
            })

    # Check if best score is above threshold and unit matches
    if best['score'] >= threshold:
        if parsed.get('unit', '') == best['aptnbr']:
            return best['hhid'], round(best['score'], 2)

    # No match found
    return None, 0.0