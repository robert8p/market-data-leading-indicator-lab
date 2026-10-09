ALTER FUNCTION public.market_data_remediation_performance_probe_v2(jsonb) SET statement_timeout='30s';
ALTER FUNCTION public.market_data_remediation_validation_step_v2(jsonb) SET statement_timeout='30s';
NOTIFY pgrst,'reload schema';
