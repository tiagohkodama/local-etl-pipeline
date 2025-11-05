-- Migration: create etl.results table
CREATE TABLE IF NOT EXISTS etl.results (
  id SERIAL PRIMARY KEY,
  date DATE NOT NULL,
  total_amount NUMERIC,
  created_at TIMESTAMP DEFAULT now()
);
