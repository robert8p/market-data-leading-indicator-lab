from pathlib import Path
import re,hashlib,json
p=Path(__file__).with_name('worker.ts');s=p.read_text();pin=re.search(r"const CANONICAL_WORKER_SHA='([0-9a-f]{64})';",s).group(1)
canonical=re.sub(r"const CANONICAL_WORKER_SHA='[0-9a-f]{64}';","const CANONICAL_WORKER_SHA='WORKER_SHA_PLACEHOLDER';",s)
actual=hashlib.sha256(canonical.encode()).hexdigest();assert pin==actual
print(json.dumps({'worker_raw_sha256':hashlib.sha256(s.encode()).hexdigest(),'worker_canonical_sha256':actual,'contract_sha256':hashlib.sha256(p.with_name('contract.mjs').read_bytes()).hexdigest(),'canonical_pin_matches':True}))
