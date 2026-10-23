select pg_advisory_xact_lock(2026083000261022);
do $guard$ begin
if not exists(select 1 from population_top3000_202608_202609_v1.acquisition_control where owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' and owner_fence=2 and stop_generation=0 and not stopped) then raise exception 'OWNER_CHANGED';end if;
end $guard$;
create index if not exists sec_filing_index_source_part_v1 on population_top3000_202608_202609_v1.sec_filing_index_v1(source_id,part_number);
