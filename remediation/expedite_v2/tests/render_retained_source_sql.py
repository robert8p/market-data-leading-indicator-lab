"""Render rollback-only tests from pinned retained-source responses fetched via MCP."""
import json,sys
from pathlib import Path
from app.remediation_expedite_v2 import retained_text
pages=json.loads(Path(sys.argv[1]).read_text())
s=Path(sys.argv[2]).read_text()
for page in pages:
    raw=retained_text(page)
    if '$raw$' in raw: raise ValueError('fixture_sql_delimiter')
    s=s.replace('{{SOURCE_RAW_'+str(page['batch_id'])+'}}',raw)
if '{{SOURCE_RAW_' in s: raise ValueError('missing_pinned_fixture')
print(s)
