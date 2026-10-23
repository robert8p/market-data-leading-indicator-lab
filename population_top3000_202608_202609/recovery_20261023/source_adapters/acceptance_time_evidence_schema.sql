-- Retain API timestamps separately from independently reconciled acceptance instants.
ALTER TABLE population_top3000_202608_202609_v1.company_filing_candidate_v1
 ADD COLUMN IF NOT EXISTS source_accepted_at timestamptz,
 ADD COLUMN IF NOT EXISTS accepted_at_basis text NOT NULL DEFAULT 'SEC_SUBMISSIONS_API_UNVERIFIED';

CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.filing_acceptance_reconciliation_v1 (
 accession text PRIMARY KEY,
 source_id text NOT NULL,
 source_sha256 text NOT NULL CHECK(source_sha256 ~ '^[0-9a-f]{64}$'),
 source_literal text NOT NULL,
 source_accepted_at timestamptz NOT NULL,
 official_index_local timestamp NOT NULL,
 official_timezone text NOT NULL CHECK(official_timezone='America/New_York'),
 accepted_at timestamptz NOT NULL,
 official_index_url text NOT NULL,
 corroborating_metadata_sha256 text NOT NULL,
 evidence jsonb NOT NULL,
 evidence_sha256 text NOT NULL CHECK(evidence_sha256 ~ '^[0-9a-f]{64}$'),
 observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(source_literal::timestamptz=source_accepted_at),
 CHECK(accepted_at=official_index_local AT TIME ZONE 'America/New_York')
);
ALTER TABLE population_top3000_202608_202609_v1.filing_acceptance_reconciliation_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE population_top3000_202608_202609_v1.filing_acceptance_reconciliation_v1 FROM PUBLIC,anon,authenticated;
GRANT SELECT ON population_top3000_202608_202609_v1.filing_acceptance_reconciliation_v1 TO service_role;
COMMENT ON COLUMN population_top3000_202608_202609_v1.company_filing_candidate_v1.accepted_at_basis IS
 'SEC_SUBMISSIONS_API_UNVERIFIED is provisional. Source API Z literals may disagree with official EDGAR acceptance times; require per-accession index/header reconciliation before timing certification. Never apply a blanket offset.';
