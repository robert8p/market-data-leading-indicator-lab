CREATE TABLE population_top3000_202608_202609_v1.ref01_date_plan_v9(
 plan_id text PRIMARY KEY, content jsonb NOT NULL, content_sha256 text NOT NULL,
 code_bundle_sha256 text NOT NULL, enabled boolean NOT NULL DEFAULT false,
 last_blocker text, updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(content_sha256=encode(sha256(convert_to(content::text,'UTF8')),'hex')),
 CHECK(code_bundle_sha256 ~ '^[0-9a-f]{64}$'),
 CHECK(jsonb_typeof(content->'dates')='array' AND jsonb_array_length(content->'dates')=60),
 CHECK(content ?& ARRAY['max_provider_calls','max_http_invocations','max_source_bytes','max_relation_bytes','max_pages_per_date','initial_page_tranche','renewal_page_tranche','owner_id','owner_fence','stop_generation','dates','input_versions']),
 CHECK((content->>'max_provider_calls')::bigint BETWEEN 1 AND 1000000 AND (content->>'max_http_invocations')::bigint BETWEEN 1 AND 1000000),
 CHECK((content->>'max_source_bytes')::bigint BETWEEN 8388608 AND 100000000000 AND (content->>'max_relation_bytes')::bigint BETWEEN 16777216 AND 1000000000000),
 CHECK((content->>'max_pages_per_date')::int BETWEEN 1 AND 100000 AND (content->>'initial_page_tranche')::int BETWEEN 1 AND 100000 AND (content->>'renewal_page_tranche')::int BETWEEN 1 AND 100000)
);
CREATE UNIQUE INDEX ref01_date_one_enabled_plan_v9 ON population_top3000_202608_202609_v1.ref01_date_plan_v9(enabled) WHERE enabled;
CREATE TABLE population_top3000_202608_202609_v1.ref01_date_plan_request_v9(
 plan_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.ref01_date_plan_v9,
 as_of date NOT NULL, request_key text NOT NULL UNIQUE REFERENCES population_top3000_202608_202609_v1.ref01_date_request_v4,
 manifest_sha256 text NOT NULL, input_versions_sha256 text NOT NULL,
 PRIMARY KEY(plan_id,as_of), CHECK(as_of BETWEEN DATE '2026-08-01' AND DATE '2026-09-30')
);
ALTER TABLE population_top3000_202608_202609_v1.ref01_date_plan_v9 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ref01_date_plan_request_v9 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ref01_date_plan_v9,population_top3000_202608_202609_v1.ref01_date_plan_request_v9 FROM PUBLIC,anon,authenticated;
