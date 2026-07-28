import importlib.util
import os
import sys
import types

os.environ.setdefault("DATABASE_URL", "postgresql://user:password@localhost:5432/postgres")
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-service-role")
os.environ.setdefault("APP_PASSWORD", "test-password")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-with-more-than-32-characters")
os.environ.setdefault("ALPACA_API_KEY", "test")
os.environ.setdefault("ALPACA_API_SECRET", "test")
os.environ.setdefault("TWELVEDATA_API_KEY", "test")
os.environ.setdefault("MASSIVE_API_KEY", "test")
os.environ.setdefault("SEC_USER_AGENT", "Tests test@example.com")

# The execution environment used to validate the source package does not ship
# PostgreSQL/TUS clients. Production installs them from requirements.txt.
if importlib.util.find_spec("psycopg") is None:
    psycopg = types.ModuleType("psycopg")
    psycopg.Connection = object
    psycopg.connect = lambda *args, **kwargs: None
    rows = types.ModuleType("psycopg.rows")
    rows.dict_row = object()
    json_mod = types.ModuleType("psycopg.types.json")
    json_mod.Jsonb = lambda value: value
    types_mod = types.ModuleType("psycopg.types")
    sys.modules["psycopg"] = psycopg
    sys.modules["psycopg.rows"] = rows
    sys.modules["psycopg.types"] = types_mod
    sys.modules["psycopg.types.json"] = json_mod

if importlib.util.find_spec("psycopg_pool") is None:
    pool = types.ModuleType("psycopg_pool")
    pool.ConnectionPool = object
    sys.modules["psycopg_pool"] = pool

if importlib.util.find_spec("tusclient") is None:
    tusclient = types.ModuleType("tusclient")
    client_mod = types.ModuleType("tusclient.client")
    client_mod.TusClient = object
    tusclient.client = client_mod
    sys.modules["tusclient"] = tusclient
    sys.modules["tusclient.client"] = client_mod
