# Import the usaddress library for parsing structured address components
import usaddress

def parse_address(record_id: str, raw_address: str) -> dict:
    """
    Use usaddress to tag and split into components.
    Returns a dict suitable for DataFrame ingestion.
    """

    # Parse the raw address into tagged components using usaddress
    parsed, _ = usaddress.tag(raw_address)

    # Extract the street number from the parsed output
    street_number = parsed.get('AddressNumber', '').strip().upper()

    # Extract the core street name (e.g., "Humboldt")
    street_name = parsed.get('StreetName', '').strip().upper()

    # Extract the street type or suffix (e.g., "Street", "Avenue")
    street_type = parsed.get('StreetNamePostType', '').strip().upper()

    # Extract apartment/unit type (e.g., "Apt", "Unit")
    apt_type = parsed.get('OccupancyType', '').strip().upper()

    # Extract apartment/unit number (e.g., "2B")
    unit = parsed.get('OccupancyIdentifier', '').strip().upper()

    # Extract directional prefix (e.g., "N", "S")
    predir =  parsed.get('StreetNamePreDirectional', '').strip().upper()

    # Extract directional suffix (e.g., "NW", "SE")
    postdir = parsed.get('StreetNamePostDirectional', '').strip().upper()


    # Return a dictionary of parsed components with original record ID and address
    return {
        'id': record_id,
        'street_number': street_number,
        'predir': predir,
        'street_name': street_name,
        'street_type': street_type,
        'postdir': postdir,
        'apt_type': apt_type,
        'unit': unit,
        'original_address': raw_address
    }
