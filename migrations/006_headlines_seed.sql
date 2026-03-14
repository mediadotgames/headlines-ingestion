-- Seed headlines from newsapi CSV into public.headlines
-- Assumes you run this with psql from the fe-api project root

BEGIN;

-- Staging table with columns matching the CSV header
CREATE TEMP TABLE tmp_headlines_raw (
    id           text,          -- CSV "id" (used as source_id)
    source_name  text,
    author       text,
    title        text,
    description  text,
    url          text,
    url_to_image text,
    published_at timestamptz,
    content      text
) ON COMMIT DROP;

-- Load the CSV (path is relative to fe-api/)
\copy tmp_headlines_raw (id, source_name, author, title, description, url, url_to_image, published_at, content) FROM './headlines_seeds.csv' WITH (FORMAT csv, HEADER true);

-- Insert into the real table, mapping CSV id -> headlines.source_id
INSERT INTO public.headlines (
    source_id,
    source_name,
    author,
    title,
    description,
    url,
    url_to_image,
    published_at,
    content
)
SELECT
    id           AS source_id,
    source_name,
    author,
    title,
    description,
    url,
    url_to_image,
    published_at,
    content
FROM tmp_headlines_raw
ON CONFLICT (url) DO NOTHING;  -- safe to re-run

COMMIT;