CREATE UNIQUE INDEX resource_probe_v9_one_measurement_per_claim_v1 ON population_top3000_202608_202609_v1.resource_probe_v9 (((authorization_receipt->>'measurement_claim_event_id')::bigint));
