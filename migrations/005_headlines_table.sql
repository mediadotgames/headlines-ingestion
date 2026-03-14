-- 005_headlines_table.sql
-- Implements a new table for headlines
-- Identity (for now): url (PRIMARY KEY)
-- Also keeps: id as a stable generated identifier (UNIQUE) for future migration

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS public.headlines (
    -- Future-friendly row identifier (not primary key yet)
    id text NOT NULL DEFAULT gen_random_uuid()::text,

    -- Upstream source fields
    source_id text,
    source_name text NOT NULL,
    author text,
    -- Headline content
    title text NOT NULL,
    description text,

    -- Identity key for now
    url text PRIMARY KEY,

    -- Use snake_case to avoid Postgres folding issues
    url_to_image text,
    published_at timestamptz NOT NULL,
    content text,


    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Add scoring / annotation columns
ALTER TABLE IF EXISTS public.headlines
    ADD COLUMN IF NOT EXISTS public_interest_score int,
    ADD COLUMN IF NOT EXISTS material_impact boolean,
    ADD COLUMN IF NOT EXISTS institutional_action boolean,
    ADD COLUMN IF NOT EXISTS scope_scale boolean,
    ADD COLUMN IF NOT EXISTS new_information boolean,
    ADD COLUMN IF NOT EXISTS topic_id UUID;

-- Ensure id is unique now, so promoting it to PRIMARY KEY later is painless
CREATE UNIQUE INDEX IF NOT EXISTS headlines_id_uniq
ON public.headlines (id);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS headlines_published_at_idx
ON public.headlines (published_at DESC);

CREATE INDEX IF NOT EXISTS headlines_source_name_idx
ON public.headlines (source_name);

-- Maintain updated_at automatically (assumes function already exists)
DROP TRIGGER IF EXISTS set_updated_at ON public.headlines;
CREATE TRIGGER set_updated_at
BEFORE UPDATE ON public.headlines
FOR EACH ROW
EXECUTE FUNCTION tg_set_updated_at();