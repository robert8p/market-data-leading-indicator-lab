"""One natural verifier attempt per immutable release, including across process restarts."""
import json
import re
import time
import uuid

PREFIX='engineering:eq17-backlog-20261023:hosted_supervision:'
PREDECESSORS={
    '96958519c5d263829f89bcb750f1c8a6217fe97cd9f774dad118c5c9a8a7098b',
    '272f18837f174747bd8ef9b9b7083deab7c3f8fc4ebb4cea13061121b4074a00',
    '296e0d08f1c065612bf3ea50368a13643b0b3d3b06cd4d8bba27e1949b71bfb6',
    '694d441a11651ceba8ede50e9080bee63772bef37020ac1d18003c90e08d2e99',
    'e360cb0758e5038011212f9f13da68d12d6f39f7b4013823a2fde4840834f493',
    '896621289e43fa8d7a675eccbfb671596b5853519ff378f14f8808342e15ea4f',
    'badf62a10ca7f5c7cf2a4249b07d8b64870d4ad2ff72de4c0b35c5b29440fb37',
    'cb383a5e04b9197bf58ec9b5177039d4a3e212080d9828edbabc0a544b2a7b89',
}
SCHEMA='eq17.verifier_once.v1'
EXECUTION='EXISTING_HOSTED_VERIFIER_NO_SCANNER'
TERMINAL={'blocked','complete','stopped'}
def valid_sha(value,length):
    return type(value) is str and re.fullmatch('[0-9a-f]{'+str(length)+'}',value) is not None
def encode(value):return json.dumps(value,sort_keys=True,allow_nan=False)
class Attempt:
    def __init__(self,release,git,expected_release):
        if release!=expected_release or not valid_sha(release,64) or release in PREDECESSORS or not valid_sha(git,40):raise ValueError('VERIFIER_UNBOUND_RELEASE')
        self.release=release
        self.git=git
        self.key=PREFIX+release
        self.nonce=uuid.uuid4().hex
        self.current=None
        self.finished=False
    def envelope(self,value):
        return dict(value,schema=SCHEMA,attempt_nonce=self.nonce,attempt=1,release=self.release,git_commit=self.git,
                    checked=time.time(),execution=EXECUTION,maximum_seconds=720)
    def claim(self,conn):
        if self.current is not None or self.finished:raise RuntimeError('VERIFIER_CLAIM_ALREADY_USED')
        initial=encode(self.envelope(dict(status='running')))
        created=conn.execute('INSERT INTO live_eq17.state(key,payload) VALUES (%s,%s) ON CONFLICT(key) DO NOTHING RETURNING key',(self.key,initial)).fetchone()
        if created==(self.key,):
            self.current=initial
            return 'claimed'
        # This process can never become owner of an existing row, including a stale, unknown running row.
        row=conn.execute("SELECT payload::jsonb->>'status' FROM live_eq17.state WHERE key=%s AND payload::jsonb->>'schema'=%s AND payload::jsonb->>'release'=%s AND payload::jsonb->>'execution'=%s AND payload::jsonb->>'attempt_nonce' ~ '^[0-9a-f]{32}$' AND payload::jsonb->'attempt'='1'::jsonb",
                         (self.key,SCHEMA,self.release,EXECUTION)).fetchone()
        return 'terminal' if row and row[0] in TERMINAL else 'held'
    def record(self,conn,value):
        status=value.get('status')
        if not self.current or self.finished or type(value.get('attempt')) is not int or value['attempt']!=1 or status not in TERMINAL|{'running'}:
            raise RuntimeError('VERIFIER_RESULT_FENCE')
        if status=='running':
            # The caller's initial advisory record is already the committed INSERT.
            return json.loads(self.current)
        if status=='complete' and (type(value.get('returncode')) is not int or value['returncode']!=0):
            raise RuntimeError('VERIFIER_RESULT_FENCE')
        updated=self.envelope(value)
        payload=encode(updated)
        row=conn.execute('UPDATE live_eq17.state SET payload=%s WHERE key=%s AND payload=%s RETURNING key',(payload,self.key,self.current)).fetchone()
        if row!=(self.key,):raise RuntimeError('VERIFIER_RESULT_FENCE')
        self.current=payload
        self.finished=status in TERMINAL
        return updated
