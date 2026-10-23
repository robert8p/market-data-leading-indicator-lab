CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.query_cursor_v1(p_url text)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE STRICT
 SET search_path TO 'pg_catalog'
AS $function$
declare encoded text;decoded bytea:=''::bytea;i integer:=1;c text;
begin
if p_url !~ '^https://api[.](massive[.]com|polygon[.]io)/v3/reference/tickers[?]' then return null;end if;
encoded:=substring(p_url from '[?&]cursor=([^&#]*)');if encoded is null then return null;end if;
while i<=length(encoded) loop
 c:=substring(encoded from i for 1);
 if c='%' then
  if substring(encoded from i+1 for 2) !~ '^[0-9a-fA-F]{2}$' then return null;end if;
  decoded:=decoded||decode(substring(encoded from i+1 for 2),'hex');i:=i+3;
 else decoded:=decoded||convert_to(case when c='+' then ' ' else c end,'UTF8');i:=i+1;
 end if;
end loop;
return convert_from(decoded,'UTF8');
exception when others then return null;
end $function$
