-- Drop existing tables if they exist to ensure a clean slate
DROP TABLE IF EXISTS raw_transactions, raw_addresses, parsed_transactions, matches;

-- Create table for raw transaction data
CREATE TABLE raw_transactions (
  id TEXT PRIMARY KEY,
  status TEXT,
  price BIGINT,
  bedrooms INT,
  bathrooms INT,
  square_feet INT,
  address_line_1 TEXT,
  address_line_2 TEXT,
  city TEXT,
  state TEXT,
  zip_code TEXT,
  property_type TEXT,
  year_built INT,
  presented_by TEXT,
  brokered_by TEXT,
  presented_by_mobile TEXT,
  mls TEXT,
  listing_office_id TEXT,
  listing_agent_id TEXT,
  created_at DATE,
  updated_at DATE,
  open_house JSONB,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  email TEXT,
  list_date DATE,
  pending_date DATE,
  presented_by_first_name TEXT,
  presented_by_last_name TEXT,
  presented_by_middle_name TEXT,
  presented_by_suffix TEXT,
  geog TEXT
);

-- Create table for raw canonical address data
CREATE TABLE raw_addresses (
  hhid TEXT PRIMARY KEY,
  fname TEXT, mname TEXT, lname TEXT, suffix TEXT,
  address TEXT,
  house TEXT, predir TEXT, street TEXT, strtype TEXT, postdir TEXT,
  apttype TEXT, aptnbr TEXT,
  city TEXT, state TEXT, zip TEXT,
  latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
  homeownercd TEXT
);

-- Create table for parsed address components from transaction data
CREATE TABLE parsed_transactions (
  id               TEXT PRIMARY KEY,
  street_number    TEXT,
  predir           TEXT,
  street_name      TEXT,
  street_type      TEXT,
  postdir          TEXT,
  apt_type         TEXT,
  unit             TEXT,
  city             TEXT,
  state            TEXT,
  zip_code         TEXT,
  original_address TEXT
);

-- Create table to store matching results
CREATE TABLE matches (
  transaction_id TEXT PRIMARY KEY,
  address_id TEXT,
  confidence FLOAT,
  match_type TEXT
);

-- Create index to optimize queries on parsed street name
CREATE INDEX idx_parsed_street_name ON parsed_transactions (street_name);

-- Create index to optimize queries on canonical street name
CREATE INDEX idx_raw_addr_street    ON raw_addresses (street);
