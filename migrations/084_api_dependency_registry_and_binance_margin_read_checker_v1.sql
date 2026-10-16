create table if not exists research_hub.api_dependency_registry_v1(
    dependency_key text primary key,
    provider text not null,
    purpose text not null,
    required_for text not null,
    status text not null,
    credential_location text,
    secret_names text[] not null default '{}'::text[],
    least_privilege_requirements text,
    exact_user_action text,
    required_now boolean not null default false,
    conditional_requirement text,
    do_not_share_in_chat boolean not null default true,
    checker_function text,
    latest_result jsonb not null default '{}'::jsonb,
    last_checked_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
revoke all on table research_hub.api_dependency_registry_v1 from public,anon,authenticated;

insert into research_hub.api_dependency_registry_v1(
    dependency_key,provider,purpose,required_for,status,credential_location,secret_names,
    least_privilege_requirements,exact_user_action,required_now,conditional_requirement,checker_function,latest_result
) values
(
    'BINANCE-PUBLIC-MARKET-DATA-V1','Binance','Public PSGUSDT bars and order-book snapshots',
    'Prospective signal replication and execution microstructure','ACTIVE_NO_CREDENTIALS_REQUIRED',null,'{}'::text[],
    'Public endpoints only','None',false,null,null,
    jsonb_build_object('active',true,'credentials_required',false)
),
(
    'BINANCE-MARGIN-PSG-ACCOUNT-READ-V1','Binance','Read-only account-specific PSG margin availability, maximum borrowable amount and indicative interest data',
    'Resolve whether the untouched-holdout PSG short can be executed through an eligible Binance Margin account',
    'CONDITIONAL_CREDENTIALS_NOT_PRESENT','Supabase Vault',array['BINANCE_MARGIN_API_KEY','BINANCE_MARGIN_API_SECRET'],
    'Dedicated Binance API key with reading/USER_DATA access only; trading and withdrawals disabled. Never reuse a general-purpose or withdrawal-enabled key.',
    'Only if your Binance account visibly has Margin access: in Supabase project oxzabweahkoimtevbbny open Database > Vault, create two named secrets BINANCE_MARGIN_API_KEY and BINANCE_MARGIN_API_SECRET. Do not paste either value into ChatGPT.',
    false,
    'Useful only if Rob has an eligible Binance account with Margin access. It is not needed for ongoing public-data research.',
    'research_hub.invoke_binance_psg_margin_access_check_v1',
    jsonb_build_object('credentials_present',false,'public_research_continues_without_credentials',true)
)
on conflict(dependency_key) do update set
    provider=excluded.provider,purpose=excluded.purpose,required_for=excluded.required_for,status=excluded.status,
    credential_location=excluded.credential_location,secret_names=excluded.secret_names,
    least_privilege_requirements=excluded.least_privilege_requirements,exact_user_action=excluded.exact_user_action,
    required_now=excluded.required_now,conditional_requirement=excluded.conditional_requirement,
    checker_function=excluded.checker_function,latest_result=research_hub.api_dependency_registry_v1.latest_result||excluded.latest_result,
    updated_at=now();

create table if not exists research_hub.binance_psg_margin_access_checks_v1(
    check_id uuid primary key default gen_random_uuid(),
    requested_at timestamptz not null default now(),
    finalized_at timestamptz,
    status text not null,
    credentials_present boolean not null default false,
    all_pairs_request_id bigint,
    all_assets_request_id bigint,
    cross_max_borrow_request_id bigint,
    isolated_max_borrow_request_id bigint,
    isolated_interest_request_id bigint,
    sanitized_result jsonb not null default '{}'::jsonb,
    error_summary jsonb not null default '{}'::jsonb,
    definition_hash text not null default md5('binance-psg-margin-read-v1|allPairs|allAssets|maxBorrow-cross|maxBorrow-isolated|next-hourly-interest|read-only'),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists binance_psg_margin_access_checks_requested_idx
    on research_hub.binance_psg_margin_access_checks_v1(requested_at desc);
revoke all on table research_hub.binance_psg_margin_access_checks_v1 from public,anon,authenticated;

create or replace function research_hub.try_jsonb_v1(p_text text)
returns jsonb
language plpgsql
immutable
set search_path=pg_catalog,pg_temp
as $$
begin
    return p_text::jsonb;
exception when others then
    return jsonb_build_object('parse_error',true,'text_prefix',left(coalesce(p_text,''),500));
end;
$$;
revoke all on function research_hub.try_jsonb_v1(text) from public,anon,authenticated;

create or replace function research_hub.binance_margin_credentials_present_v1()
returns jsonb
language plpgsql
security definer
set search_path=pg_catalog,vault,research_hub,pg_temp
as $$
declare
    v_key_present boolean;
    v_secret_present boolean;
begin
    select exists(select 1 from vault.decrypted_secrets where name='BINANCE_MARGIN_API_KEY' and nullif(decrypted_secret,'') is not null),
           exists(select 1 from vault.decrypted_secrets where name='BINANCE_MARGIN_API_SECRET' and nullif(decrypted_secret,'') is not null)
    into v_key_present,v_secret_present;
    return jsonb_build_object(
        'api_key_present',v_key_present,
        'api_secret_present',v_secret_present,
        'credentials_present',v_key_present and v_secret_present
    );
end;
$$;
revoke all on function research_hub.binance_margin_credentials_present_v1() from public,anon,authenticated;

create or replace function research_hub.binance_signed_query_v1(p_query text,p_secret text)
returns text
language sql
immutable
set search_path=pg_catalog,extensions,pg_temp
as $$
select p_query||'&signature='||encode(extensions.hmac(convert_to(p_query,'UTF8'),convert_to(p_secret,'UTF8'),'sha256'),'hex')
$$;
revoke all on function research_hub.binance_signed_query_v1(text,text) from public,anon,authenticated;

create or replace function research_hub.invoke_binance_psg_margin_access_check_v1()
returns jsonb
language plpgsql
security definer
set search_path=pg_catalog,public,research_hub,vault,net,pg_temp
as $$
declare
    v_api_key text;
    v_api_secret text;
    v_ts bigint;
    v_q text;
    v_check_id uuid;
    v_pairs bigint;
    v_assets bigint;
    v_cross bigint;
    v_isolated bigint;
    v_interest bigint;
    v_recent timestamptz;
begin
    select decrypted_secret into v_api_key from vault.decrypted_secrets where name='BINANCE_MARGIN_API_KEY' limit 1;
    select decrypted_secret into v_api_secret from vault.decrypted_secrets where name='BINANCE_MARGIN_API_SECRET' limit 1;

    if nullif(v_api_key,'') is null or nullif(v_api_secret,'') is null then
        update research_hub.api_dependency_registry_v1
        set status='CONDITIONAL_CREDENTIALS_NOT_PRESENT',required_now=false,
            latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object('credentials_present',false,'last_probe_at',now()),
            last_checked_at=now(),updated_at=now()
        where dependency_key='BINANCE-MARGIN-PSG-ACCOUNT-READ-V1';
        return jsonb_build_object('status','credentials_missing','required_now',false,'public_research_continues',true);
    end if;

    select max(requested_at) into v_recent
    from research_hub.binance_psg_margin_access_checks_v1
    where status in ('REQUESTED','FINALIZED','ACCOUNT_BORROW_CONFIRMED','GLOBAL_SUPPORT_ACCOUNT_CAPACITY_UNCONFIRMED')
      and requested_at>now()-interval '20 hours';
    if v_recent is not null then
        return jsonb_build_object('status','recent_check_exists','latest_requested_at',v_recent);
    end if;

    v_ts:=(extract(epoch from clock_timestamp())*1000)::bigint;

    v_q:='recvWindow=5000&timestamp='||v_ts;
    select net.http_get(
        url:='https://api.binance.com/sapi/v1/margin/allPairs?'||research_hub.binance_signed_query_v1(v_q,v_api_secret),
        headers:=jsonb_build_object('X-MBX-APIKEY',v_api_key),timeout_milliseconds:=15000
    ) into v_pairs;

    v_q:='recvWindow=5000&timestamp='||v_ts;
    select net.http_get(
        url:='https://api.binance.com/sapi/v1/margin/allAssets?'||research_hub.binance_signed_query_v1(v_q,v_api_secret),
        headers:=jsonb_build_object('X-MBX-APIKEY',v_api_key),timeout_milliseconds:=15000
    ) into v_assets;

    v_q:='asset=PSG&recvWindow=5000&timestamp='||v_ts;
    select net.http_get(
        url:='https://api.binance.com/sapi/v1/margin/maxBorrowable?'||research_hub.binance_signed_query_v1(v_q,v_api_secret),
        headers:=jsonb_build_object('X-MBX-APIKEY',v_api_key),timeout_milliseconds:=15000
    ) into v_cross;

    v_q:='asset=PSG&isolatedSymbol=PSGUSDT&recvWindow=5000&timestamp='||v_ts;
    select net.http_get(
        url:='https://api.binance.com/sapi/v1/margin/maxBorrowable?'||research_hub.binance_signed_query_v1(v_q,v_api_secret),
        headers:=jsonb_build_object('X-MBX-APIKEY',v_api_key),timeout_milliseconds:=15000
    ) into v_isolated;

    v_q:='assets=PSG&isIsolated=TRUE&recvWindow=5000&timestamp='||v_ts;
    select net.http_get(
        url:='https://api.binance.com/sapi/v1/margin/next-hourly-interest-rate?'||research_hub.binance_signed_query_v1(v_q,v_api_secret),
        headers:=jsonb_build_object('X-MBX-APIKEY',v_api_key),timeout_milliseconds:=15000
    ) into v_interest;

    insert into research_hub.binance_psg_margin_access_checks_v1(
        status,credentials_present,all_pairs_request_id,all_assets_request_id,
        cross_max_borrow_request_id,isolated_max_borrow_request_id,isolated_interest_request_id
    ) values('REQUESTED',true,v_pairs,v_assets,v_cross,v_isolated,v_interest)
    returning check_id into v_check_id;

    update research_hub.api_dependency_registry_v1
    set status='CREDENTIALS_PRESENT_CHECK_REQUESTED',required_now=false,
        latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object('credentials_present',true,'check_id',v_check_id,'requested_at',now()),
        last_checked_at=now(),updated_at=now()
    where dependency_key='BINANCE-MARGIN-PSG-ACCOUNT-READ-V1';

    return jsonb_build_object('status','requested','check_id',v_check_id,'request_ids',jsonb_build_array(v_pairs,v_assets,v_cross,v_isolated,v_interest));
end;
$$;
revoke all on function research_hub.invoke_binance_psg_margin_access_check_v1() from public,anon,authenticated;

create or replace function research_hub.finalize_binance_psg_margin_access_check_v1()
returns jsonb
language plpgsql
security definer
set search_path=pg_catalog,public,research_hub,net,pg_temp
as $$
declare
    c research_hub.binance_psg_margin_access_checks_v1%rowtype;
    v_pairs_status integer; v_assets_status integer; v_cross_status integer; v_isolated_status integer; v_interest_status integer;
    v_pairs jsonb; v_assets jsonb; v_cross jsonb; v_isolated jsonb; v_interest jsonb;
    v_pair jsonb; v_asset jsonb; v_interest_row jsonb;
    v_cross_amount numeric; v_isolated_amount numeric;
    v_pair_sell boolean:=false; v_pair_buy boolean:=false; v_asset_borrow boolean:=false;
    v_status text; v_sanitized jsonb; v_errors jsonb;
begin
    select * into c
    from research_hub.binance_psg_margin_access_checks_v1
    where status='REQUESTED'
    order by requested_at desc
    limit 1
    for update;
    if c.check_id is null then return jsonb_build_object('status','no_requested_check'); end if;

    select status_code,research_hub.try_jsonb_v1(content) into v_pairs_status,v_pairs from net._http_response where id=c.all_pairs_request_id;
    select status_code,research_hub.try_jsonb_v1(content) into v_assets_status,v_assets from net._http_response where id=c.all_assets_request_id;
    select status_code,research_hub.try_jsonb_v1(content) into v_cross_status,v_cross from net._http_response where id=c.cross_max_borrow_request_id;
    select status_code,research_hub.try_jsonb_v1(content) into v_isolated_status,v_isolated from net._http_response where id=c.isolated_max_borrow_request_id;
    select status_code,research_hub.try_jsonb_v1(content) into v_interest_status,v_interest from net._http_response where id=c.isolated_interest_request_id;

    if v_pairs_status is null or v_assets_status is null or v_cross_status is null or v_isolated_status is null or v_interest_status is null then
        return jsonb_build_object('status','responses_pending','check_id',c.check_id);
    end if;

    if jsonb_typeof(v_pairs)='array' then
        select x into v_pair from jsonb_array_elements(v_pairs) x where x->>'symbol'='PSGUSDT' limit 1;
    end if;
    if jsonb_typeof(v_assets)='array' then
        select x into v_asset from jsonb_array_elements(v_assets) x where coalesce(x->>'assetName',x->>'asset')='PSG' limit 1;
    end if;
    if jsonb_typeof(v_interest)='array' then
        select x into v_interest_row from jsonb_array_elements(v_interest) x where coalesce(x->>'asset',x->>'assetName')='PSG' limit 1;
    end if;

    begin v_pair_sell:=coalesce((v_pair->>'isSellAllowed')::boolean,false); exception when others then v_pair_sell:=false; end;
    begin v_pair_buy:=coalesce((v_pair->>'isBuyAllowed')::boolean,false); exception when others then v_pair_buy:=false; end;
    begin v_asset_borrow:=coalesce((v_asset->>'isBorrowable')::boolean,false); exception when others then v_asset_borrow:=false; end;
    begin v_cross_amount:=nullif(v_cross->>'amount','')::numeric; exception when others then v_cross_amount:=null; end;
    begin v_isolated_amount:=nullif(v_isolated->>'amount','')::numeric; exception when others then v_isolated_amount:=null; end;

    v_errors:=jsonb_build_object(
        'allPairs',case when v_pairs_status=200 then null else jsonb_build_object('http',v_pairs_status,'code',v_pairs->'code','message',v_pairs->'msg') end,
        'allAssets',case when v_assets_status=200 then null else jsonb_build_object('http',v_assets_status,'code',v_assets->'code','message',v_assets->'msg') end,
        'crossMaxBorrow',case when v_cross_status=200 then null else jsonb_build_object('http',v_cross_status,'code',v_cross->'code','message',v_cross->'msg') end,
        'isolatedMaxBorrow',case when v_isolated_status=200 then null else jsonb_build_object('http',v_isolated_status,'code',v_isolated->'code','message',v_isolated->'msg') end,
        'isolatedInterest',case when v_interest_status=200 then null else jsonb_build_object('http',v_interest_status,'code',v_interest->'code','message',v_interest->'msg') end
    );

    v_status:=case
        when v_pairs_status in (401,403,451) or v_assets_status in (401,403,451) or v_cross_status in (401,403,451) or v_isolated_status in (401,403,451)
            then 'ACCOUNT_OR_JURISDICTION_ACCESS_RESTRICTED'
        when coalesce(v_pairs->>'code','') in ('-2014','-2015','-1022') or coalesce(v_cross->>'code','') in ('-2014','-2015','-1022')
            then 'CREDENTIALS_INVALID_OR_PERMISSION_RESTRICTED'
        when v_pair is null or v_asset is null
            then 'PSG_MARGIN_PAIR_OR_ASSET_NOT_AVAILABLE'
        when v_pair_sell and v_asset_borrow and greatest(coalesce(v_cross_amount,0),coalesce(v_isolated_amount,0))>0
            then 'ACCOUNT_BORROW_CONFIRMED'
        when v_pair_sell and v_asset_borrow
            then 'GLOBAL_SUPPORT_ACCOUNT_CAPACITY_UNCONFIRMED'
        else 'PSG_MARGIN_NOT_BORROWABLE'
    end;

    v_sanitized:=jsonb_build_object(
        'pair_found',v_pair is not null,
        'asset_found',v_asset is not null,
        'pair_is_sell_allowed',v_pair_sell,
        'pair_is_buy_allowed',v_pair_buy,
        'asset_is_borrowable',v_asset_borrow,
        'cross_max_borrowable_psg',v_cross_amount,
        'isolated_max_borrowable_psg',v_isolated_amount,
        'isolated_hourly_interest_rate',coalesce(v_interest_row->'nextHourlyInterestRate',v_interest_row->'hourlyInterestRate'),
        'responses_http',jsonb_build_object('allPairs',v_pairs_status,'allAssets',v_assets_status,'crossMaxBorrow',v_cross_status,'isolatedMaxBorrow',v_isolated_status,'isolatedInterest',v_interest_status),
        'checked_at',now(),
        'no_orders_or_transfers_executed',true
    );

    update research_hub.binance_psg_margin_access_checks_v1
    set status=v_status,finalized_at=now(),sanitized_result=v_sanitized,error_summary=v_errors,updated_at=now()
    where check_id=c.check_id;

    update research_hub.api_dependency_registry_v1
    set status=v_status,required_now=false,latest_result=v_sanitized||jsonb_build_object('check_id',c.check_id,'error_summary',v_errors),
        last_checked_at=now(),updated_at=now()
    where dependency_key='BINANCE-MARGIN-PSG-ACCOUNT-READ-V1';

    update research_hub.merp_psg_execution_validation_v1
    set short_access_status=v_status,
        blockers=case when v_status='ACCOUNT_BORROW_CONFIRMED' then blockers-'No verified UK-retail PSG short/borrow route has been established' else blockers end,
        updated_at=now()
    where candidate_id='RH-1F6255D317EE';

    update research_hub.program_jobs
    set current_state=case
            when v_status='ACCOUNT_BORROW_CONFIRMED' then 'binance_account_borrow_confirmed_microstructure_and_compliance_validation_continue'
            when v_status='GLOBAL_SUPPORT_ACCOUNT_CAPACITY_UNCONFIRMED' then 'binance_margin_globally_supported_account_capacity_unconfirmed'
            else 'prospective_execution_validation_short_access_unresolved' end,
        latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object('binance_margin_account_read',v_sanitized||jsonb_build_object('status',v_status,'error_summary',v_errors)),
        next_automatic_action=case
            when v_status='ACCOUNT_BORROW_CONFIRMED' then 'Continue prospective microstructure and signal replication; separately confirm the account/service is compliant for the UK user before any live use.'
            else 'Continue public-data validation. Do not assume PSG short access from exchange metadata alone.' end,
        intervention_required=false,exact_intervention=null,updated_at=now()
    where job_key='EXEC-MERP-PSG-V1';

    return jsonb_build_object('status',v_status,'check_id',c.check_id,'sanitized_result',v_sanitized,'error_summary',v_errors);
end;
$$;
revoke all on function research_hub.finalize_binance_psg_margin_access_check_v1() from public,anon,authenticated;

create or replace function research_hub.get_api_actions_required_v1()
returns jsonb
language sql
stable
set search_path=pg_catalog,research_hub,pg_temp
as $$
select coalesce(jsonb_agg(jsonb_build_object(
    'dependency_key',dependency_key,'provider',provider,'purpose',purpose,'status',status,
    'required_now',required_now,'conditional_requirement',conditional_requirement,
    'credential_location',credential_location,'secret_names',secret_names,
    'least_privilege_requirements',least_privilege_requirements,'exact_user_action',exact_user_action,
    'do_not_share_in_chat',do_not_share_in_chat
) order by required_now desc,dependency_key),'[]'::jsonb)
from research_hub.api_dependency_registry_v1
where required_now=true or status like 'CONDITIONAL%'
$$;
revoke all on function research_hub.get_api_actions_required_v1() from public,anon,authenticated;

do $do$
begin
    if exists(select 1 from cron.job where jobname='research_hub_binance_psg_margin_access_invoke_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_binance_psg_margin_access_invoke_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_binance_psg_margin_access_invoke_v1','7,37 * * * *',
        'select research_hub.invoke_binance_psg_margin_access_check_v1();'
    );
    if exists(select 1 from cron.job where jobname='research_hub_binance_psg_margin_access_finalize_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_binance_psg_margin_access_finalize_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_binance_psg_margin_access_finalize_v1','12,42 * * * *',
        'select research_hub.finalize_binance_psg_margin_access_check_v1();'
    );
end $do$;
