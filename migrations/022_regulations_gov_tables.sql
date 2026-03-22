-- initdb/022_regulations_gov_tables.sql
-- -----------------------------------------------------------------------------
-- Purpose:
--   Core tables for regulations.gov data collection: dockets, documents,
--   comment counts (received, posted, document-level), and collection run metadata.
--
-- Conventions:
--   - Text PKs (API-provided identifiers, not UUIDs)
--   - raw_json JSONB column on entity tables for future-proofing
--   - UTC everywhere (timestamptz)
--   - ON CONFLICT DO UPDATE upsert pattern
--   - Reuses tg_set_updated_at() trigger from 001_schema.sql
-- -----------------------------------------------------------------------------

-- -----------------------------
-- Dockets
-- -----------------------------
CREATE TABLE IF NOT EXISTS reg_dockets (
  docket_id           text PRIMARY KEY,
  agency_id           text NOT NULL,
  docket_type         text NOT NULL,
  title               text,
  short_title         text,
  dk_abstract         text,
  modify_date         timestamptz,
  effective_date      timestamptz,
  keywords            text[],
  rin                 text,
  organization        text,
  category            text,
  generic             text,
  program             text,
  petition_nbr        text,
  legacy_id           text,
  sub_type            text,
  sub_type2           text,
  field1              text,
  field2              text,
  object_id           text,
  display_properties  jsonb,
  received_count      integer NOT NULL DEFAULT 0,
  posted_count        integer NOT NULL DEFAULT 0,
  raw_json            jsonb NOT NULL,

  collected_at        timestamptz NOT NULL DEFAULT now(),
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE reg_dockets IS 'Dockets from regulations.gov API (/v4/dockets)';
COMMENT ON COLUMN reg_dockets.docket_id IS 'API-provided docket identifier (e.g. EPA-HQ-OAR-2003-0129)';
COMMENT ON COLUMN reg_dockets.raw_json IS 'Complete API response body preserved for future schema evolution';

CREATE INDEX IF NOT EXISTS idx_reg_dockets_agency_id ON reg_dockets (agency_id);
CREATE INDEX IF NOT EXISTS idx_reg_dockets_keywords_gin ON reg_dockets USING GIN (keywords);
CREATE INDEX IF NOT EXISTS idx_reg_dockets_received_count ON reg_dockets (received_count DESC);
CREATE INDEX IF NOT EXISTS idx_reg_dockets_posted_count ON reg_dockets (posted_count DESC);
CREATE INDEX IF NOT EXISTS idx_reg_dockets_docket_type ON reg_dockets (docket_type);
CREATE INDEX IF NOT EXISTS idx_reg_dockets_modify_date ON reg_dockets (modify_date DESC);

DROP TRIGGER IF EXISTS set_updated_at ON reg_dockets;
CREATE TRIGGER set_updated_at
  BEFORE UPDATE ON reg_dockets
  FOR EACH ROW EXECUTE FUNCTION tg_set_updated_at();

-- -----------------------------
-- Documents
-- -----------------------------
CREATE TABLE IF NOT EXISTS reg_documents (
  document_id         text PRIMARY KEY,
  docket_id           text REFERENCES reg_dockets(docket_id),
  agency_id           text NOT NULL,
  document_type       text NOT NULL,
  title               text NOT NULL,
  object_id           text NOT NULL,
  fr_doc_num          text,
  posted_date         date NOT NULL,
  last_modified       timestamptz,
  comment_start       timestamptz,
  comment_end         timestamptz,
  open_for_comment    boolean NOT NULL DEFAULT false,
  allow_late_comments boolean NOT NULL DEFAULT false,
  within_comment_period boolean NOT NULL DEFAULT false,
  withdrawn           boolean NOT NULL DEFAULT false,
  subtype             text,
  comment_count       integer NOT NULL DEFAULT 0,
  raw_json            jsonb NOT NULL,

  collected_at        timestamptz NOT NULL DEFAULT now(),
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE reg_documents IS 'Documents from regulations.gov API (/v4/documents)';
COMMENT ON COLUMN reg_documents.document_id IS 'API-provided document identifier';
COMMENT ON COLUMN reg_documents.docket_id IS 'FK to reg_dockets; nullable for documents without a docket';
COMMENT ON COLUMN reg_documents.object_id IS 'Internal object ID used for comments-counts endpoint';
COMMENT ON COLUMN reg_documents.raw_json IS 'Complete API response body preserved for future schema evolution';

CREATE INDEX IF NOT EXISTS idx_reg_documents_docket_id ON reg_documents (docket_id);
CREATE INDEX IF NOT EXISTS idx_reg_documents_agency_id ON reg_documents (agency_id);
CREATE INDEX IF NOT EXISTS idx_reg_documents_posted_date ON reg_documents (posted_date);
CREATE INDEX IF NOT EXISTS idx_reg_documents_comment_count ON reg_documents (comment_count DESC);
CREATE INDEX IF NOT EXISTS idx_reg_documents_document_type ON reg_documents (document_type);
CREATE INDEX IF NOT EXISTS idx_reg_documents_object_id ON reg_documents (object_id);
CREATE INDEX IF NOT EXISTS idx_reg_documents_open_for_comment ON reg_documents (open_for_comment) WHERE open_for_comment = true;
CREATE INDEX IF NOT EXISTS idx_reg_documents_comment_end ON reg_documents (comment_end DESC) WHERE comment_end IS NOT NULL;

DROP TRIGGER IF EXISTS set_updated_at ON reg_documents;
CREATE TRIGGER set_updated_at
  BEFORE UPDATE ON reg_documents
  FOR EACH ROW EXECUTE FUNCTION tg_set_updated_at();

-- -----------------------------
-- Comment Counts
-- -----------------------------
-- Tracks three distinct count types:
--   'docket_received' → GET /v4/docket-comments-received-counts/{id}
--   'docket_posted'   → GET /v4/docket-comments-counts/{id}
--   'document'        → GET /v4/comments-counts/{objectId}
CREATE TABLE IF NOT EXISTS reg_comment_counts (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type     text NOT NULL,
  entity_id       text NOT NULL,
  comment_count   integer NOT NULL,

  collected_at    timestamptz NOT NULL DEFAULT now(),
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT uq_reg_comment_counts_entity UNIQUE (entity_type, entity_id),
  CONSTRAINT chk_reg_comment_counts_type CHECK (entity_type IN ('docket_received', 'docket_posted', 'document'))
);

COMMENT ON TABLE reg_comment_counts IS 'Comment counts from regulations.gov (three types: docket received, docket posted, document)';
COMMENT ON COLUMN reg_comment_counts.entity_type IS 'docket_received | docket_posted | document';
COMMENT ON COLUMN reg_comment_counts.entity_id IS 'docket_id (for docket types) or document object_id (for document type)';

CREATE INDEX IF NOT EXISTS idx_reg_comment_counts_entity ON reg_comment_counts (entity_type, entity_id);

DROP TRIGGER IF EXISTS set_updated_at ON reg_comment_counts;
CREATE TRIGGER set_updated_at
  BEFORE UPDATE ON reg_comment_counts
  FOR EACH ROW EXECUTE FUNCTION tg_set_updated_at();

-- -----------------------------
-- Collection Runs
-- -----------------------------
CREATE TABLE IF NOT EXISTS reg_collection_runs (
  run_id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  started_at          timestamptz NOT NULL DEFAULT now(),
  ended_at            timestamptz,
  status              text NOT NULL DEFAULT 'running',
  documents_collected integer NOT NULL DEFAULT 0,
  dockets_collected   integer NOT NULL DEFAULT 0,
  comments_collected  integer NOT NULL DEFAULT 0,
  errors_count        integer NOT NULL DEFAULT 0,
  config              jsonb,
  last_document_date  date,
  collection_start    date,
  collection_end      date,

  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT chk_reg_collection_runs_status CHECK (status IN ('running', 'completed', 'failed', 'interrupted'))
);

COMMENT ON TABLE reg_collection_runs IS 'Metadata for regulations.gov collection runs (resume and incremental support)';
COMMENT ON COLUMN reg_collection_runs.last_document_date IS 'Oldest document posted_date from this run, used for incremental resume';
COMMENT ON COLUMN reg_collection_runs.config IS 'Snapshot of collection configuration (date range, rate limits, etc.)';

CREATE INDEX IF NOT EXISTS idx_reg_collection_runs_status ON reg_collection_runs (status);

DROP TRIGGER IF EXISTS set_updated_at ON reg_collection_runs;
CREATE TRIGGER set_updated_at
  BEFORE UPDATE ON reg_collection_runs
  FOR EACH ROW EXECUTE FUNCTION tg_set_updated_at();
