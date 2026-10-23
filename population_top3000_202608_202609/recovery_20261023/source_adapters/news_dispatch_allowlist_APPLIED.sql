CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(p_attempt uuid, p_token text, p_slug text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;rid bigint;c population_top3000_202608_202609_v1.acquisition_control%rowtype;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 if p_slug not in('population-top3000-evidence-v1','population-top3000-price-v1','population-top3000-options-v1','population-top3000-actions-v1','population-top3000-news-v1') then raise exception 'WORKER_NOT_ADMITTED';end if;
 select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton;
 if c.http_invocations>=c.max_http_invocations then return jsonb_build_object('state','BLOCKED','blocker_code','HTTP_INVOCATION_CAP','resumption_condition',jsonb_build_object('required_action','Review measured cost and admit remaining bounded work'));end if;
 select net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/'||p_slug,headers:='{"Content-Type":"application/json"}',body:=jsonb_build_object('attempt_id',p_attempt,'token',p_token),timeout_milliseconds:=55000) into rid;
 update population_top3000_202608_202609_v1.acquisition_control set http_invocations=http_invocations+1 where singleton;
 update population_top3000_202608_202609_v1.pipeline_attempt set request_id=rid where attempt_id=p_attempt;
 return jsonb_build_object('state','ASYNC','request_id',rid,'worker',p_slug);
end $function$
;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(uuid,text,text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION population_top3000_202608_202609_v1.pipeline_dispatch_edge_v1(uuid,text,text) TO postgres,service_role;
