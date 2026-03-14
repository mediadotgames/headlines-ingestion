--Implements a new table for outlets

-- new column names

-- -----------------------------
-- Outlets
-- -----------------------------

DO $$
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'api_access_type') THEN
        CREATE TYPE api_access_type AS ENUM ('Free', 'Paid', 'No API', 'Pending');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'paywall_type') THEN
        CREATE TYPE paywall_type AS ENUM ('Hard', 'Soft', 'No Paywall', 'Pending');
    END IF;
END $$;

-- Main table
CREATE TABLE IF NOT EXISTS outlets (
    id    text                PRIMARY KEY,
    name         text                NOT NULL,
    description  text,
    url      text,
    category     text,
    language     text,
    country      text,
    list1        boolean             NOT NULL DEFAULT false,
    wh_2022      boolean             NOT NULL DEFAULT false,
    wh_2025      boolean             NOT NULL DEFAULT false,
    list2        boolean             NOT NULL DEFAULT false,
    wire         boolean             NOT NULL DEFAULT false,
    global       boolean             NOT NULL DEFAULT false,
    state_funded boolean             NOT NULL DEFAULT false,
    list3        boolean             NOT NULL DEFAULT false,
    api_access   api_access_type     NOT NULL DEFAULT 'Pending',
    paywall      paywall_type        NOT NULL DEFAULT 'Pending',
    active      boolean             NOT NULL DEFAULT true,
      -- Audit
     created_at timestamptz          NOT NULL DEFAULT now(),
     updated_at timestamptz          NOT NULL DEFAULT now(),

    CONSTRAINT outlets_name_uniq UNIQUE (name),
    CONSTRAINT outlets_url_uniq UNIQUE (url)
);

COMMENT ON TABLE outlets is 'News Outlets ingestable by media dot games';
COMMENT ON COLUMN outlets.name          IS  'Name of news outlet';
COMMENT ON COLUMN outlets.url       IS  'URL of article source';
COMMENT ON COLUMN outlets.list1         IS  'Outlet has permanent White House Press Credentials';
COMMENT ON COLUMN outlets.wh_2022       IS  'Outlet granted White House Press Credentials as of 2022';
COMMENT ON COLUMN outlets.wh_2025       IS  'Outlet granted White House Press Credentials as of 2025';
COMMENT ON COLUMN outlets.list2         IS  'Outlet has global bureaus, correspondence, coverage spans US and at least on more of: Europe, Asia, Africa, and Middle East.';
COMMENT ON COLUMN outlets.wire          IS  'Outlet produces news for syndication to other outlets';
COMMENT ON COLUMN outlets.global        IS  'Outlet covers US stories, has global bureau, and also covers some non-US stories, and has institutional memberships. ';
COMMENT ON COLUMN outlets.state_funded  IS  'Outlet receives government funding';
COMMENT ON COLUMN outlets.list3         IS  'Outlet consistently shapes public opinion by synthesizing from other sources and receives high engagement from readership';
COMMENT ON COLUMN outlets.api_access    IS  'Whether outlet itself published dev API for accessing news information';
COMMENT ON COLUMN outlets.paywall       IS  'Whether bulk of online content is behind a paywall or metered access';


-- Sync / tombstone support (Policy A): rows absent from the Sheet can be marked inactive
ALTER TABLE outlets
ADD COLUMN IF NOT EXISTS active boolean NOT NULL DEFAULT true;

COMMENT ON COLUMN outlets.active IS 'Whether this outlet is active in the Google Sheet source of truth';

CREATE INDEX IF NOT EXISTS outlets_active_idx ON outlets(active);

DROP TRIGGER IF EXISTS set_updated_at ON outlets;

CREATE TRIGGER set_updated_at
BEFORE UPDATE ON outlets
FOR EACH ROW
EXECUTE FUNCTION tg_set_updated_at();
