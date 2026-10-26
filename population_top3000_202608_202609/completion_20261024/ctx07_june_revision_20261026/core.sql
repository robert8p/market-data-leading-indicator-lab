CREATE FUNCTION population_top3000_202608_202609_v1.ctx07_june_revision_extract_v7(p_body bytea) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog AS $f$
DECLARE h text;r text;hdr text;cell text;k text;n int;vals text[]:=ARRAY['$36.47','$37.49','$37.60','$37.62'];head text[]:=ARRAY['July%2025%','May%2026%','June%2026%','July%2026%'];headers jsonb:='[]';cells jsonb:='[]';pos int;
BEGIN
IF p_body IS NULL OR octet_length(p_body)>2097152 THEN RETURN NULL;END IF;
BEGIN h:=convert_from(p_body,'UTF8');EXCEPTION WHEN character_not_in_repertoire THEN RETURN NULL;END;
IF strpos(h,'Summary table B. Establishment data, seasonally adjusted')=0 OR h NOT LIKE '%id="ces_table10.r.4"%HOURS AND EARNINGS%ALL EMPLOYEES%' OR h NOT LIKE '%id="ces_table10.r.4.1"%Total private%' OR regexp_replace(regexp_replace(h,'<[^>]*>',' ','g'),'[[:space:]]+',' ','g') NOT LIKE '%8:30 a.m. (ET) Friday, August 7, 2026%' THEN RETURN NULL;END IF;
k:='id="ces_table10.r.4.1.2">';IF (length(h)-length(replace(h,k,'')))/length(k)<>1 THEN RETURN NULL;END IF;
pos:=strpos(h,k);r:=split_part(h,k,2);IF strpos(r,'</tr>')=0 OR strpos(r,'</tr>')>2048 THEN RETURN NULL;END IF;r:=k||split_part(r,'</tr>',1)||'</tr>';
IF r NOT LIKE '%Average hourly earnings%' OR octet_length(r)<>463 OR encode(sha256(convert_to(r,'UTF8')),'hex')<>'797c6291a24d8c63f641fd1f15856e50c9dbaf7e84a0821de8528d09786ef846' THEN RETURN NULL;END IF;
FOR n IN 2..5 LOOP
k:='id="ces_table10.h.1.'||n||'">';IF (length(h)-length(replace(h,k,'')))/length(k)<>1 THEN RETURN NULL;END IF;
hdr:=split_part(h,k,2);IF strpos(hdr,'</th>')=0 OR strpos(hdr,'</th>')>1024 THEN RETURN NULL;END IF;hdr:=split_part(hdr,'</th>',1);
IF hdr LIKE '%<th%' OR hdr LIKE '%</tr%' OR regexp_replace(regexp_replace(hdr,'<[^>]*>',' ','g'),'[[:space:]]+',' ','g') NOT LIKE head[n-1] OR (n IN(4,5) AND hdr NOT LIKE '%title="Preliminary"%') THEN RETURN NULL;END IF;
k:='headers="ces_table10.r.4.1.2 ces_table10.h.1.'||n||'">';IF (length(r)-length(replace(r,k,'')))/length(k)<>1 THEN RETURN NULL;END IF;
cell:=split_part(r,k,2);IF strpos(cell,'</td>')=0 OR strpos(cell,'</td>')>256 THEN RETURN NULL;END IF;cell:=split_part(cell,'</td>',1);
IF cell LIKE '%<td%' OR cell LIKE '%</tr%' OR regexp_replace(regexp_replace(cell,'<[^>]*>','','g'),'[[:space:]]','','g')<>vals[n-1] THEN RETURN NULL;END IF;
headers:=headers||jsonb_build_object('column',n,'raw',hdr);cells:=cells||jsonb_build_object('column',n,'raw',cell);
END LOOP;
RETURN jsonb_build_object('value',37.60::numeric,'units','USD/hour','value_lexeme','37.60','reference_period','2026-06-01T04:00:00Z','publication_supported_available_at','2026-08-07T12:30:00Z','source_sha256',encode(sha256(p_body),'hex'),'raw_row_sha256',encode(sha256(convert_to(r,'UTF8')),'hex'),'raw_row_bytes',octet_length(r),'raw_row_character_start',pos,'raw_row_byte_start',octet_length(substring(h FROM 1 FOR pos-1))+1,'column',4,'headers',headers,'cells',cells);
END $f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_june_revision_extract_v7(bytea) FROM PUBLIC,anon,authenticated;
CREATE TABLE population_top3000_202608_202609_v1.ctx07_june_revision_evidence_v7(record_id text PRIMARY KEY,evidence jsonb NOT NULL,evidence_sha256 text NOT NULL,
CHECK((record_id=encode(sha256(convert_to(evidence::text,'UTF8')),'hex') AND evidence_sha256=record_id AND record_id='8bd15cd681e52d5273690cb346534a89c408fab30ef4d6c0129b87c84db7b0fe') IS TRUE),
CHECK((evidence->>'status'='NUMERICAL_PUBLICATION_RECONSTRUCTION_VERIFIED' AND evidence->>'reference_period'='2026-06-01T04:00:00Z' AND evidence->>'source_sha256'='4602e50c53ccfc789d52ef73191171d9918141516b7734524e70f2b8326024a5' AND (evidence->>'retained_numeric')::numeric=37.60 AND evidence->>'retained_units'='USD/hour' AND evidence->>'factor_name'='average_hourly_earnings_as_released' AND evidence->>'publication_supported_available_at'='2026-08-07T12:30:00Z' AND evidence->>'record_role'='revision' AND jsonb_typeof(evidence->'extraction')='object' AND evidence->>'document_charset_qualified'='false' AND evidence->>'full_vintage_denominator_qualified'='false') IS TRUE),
CHECK((evidence->>'strict_original_receipt_qualified'='false' AND evidence->>'full_family_accepted'='false' AND evidence->>'final_delivery'='false' AND evidence->'historical_received_at'='null'::jsonb) IS TRUE));
ALTER TABLE population_top3000_202608_202609_v1.ctx07_june_revision_evidence_v7 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ctx07_june_revision_evidence_v7 FROM PUBLIC,anon,authenticated;
CREATE FUNCTION population_top3000_202608_202609_v1.ctx07_reference_period_asof_v7(p_factor text,p_reference timestamptz,p_known timestamptz) RETURNS SETOF jsonb LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $f$
SELECT evidence FROM (
 SELECT evidence,record_id FROM population_top3000_202608_202609_v1.ctx07_june_revision_evidence_v7
 UNION ALL SELECT evidence,observation_id FROM population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6 WHERE evidence->>'source_id'='empsit_202607' AND evidence->>'status'='NUMERICAL_PUBLICATION_RECONSTRUCTION_VERIFIED'
) q WHERE evidence->>'status'='NUMERICAL_PUBLICATION_RECONSTRUCTION_VERIFIED' AND evidence->>'factor_name'=p_factor AND (evidence->>'reference_period')::timestamptz=p_reference AND (evidence->>'publication_supported_available_at')::timestamptz<=p_known
ORDER BY (evidence->>'publication_supported_available_at')::timestamptz DESC,record_id LIMIT 1;
$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_reference_period_asof_v7(text,timestamptz,timestamptz) FROM PUBLIC,anon,authenticated;
