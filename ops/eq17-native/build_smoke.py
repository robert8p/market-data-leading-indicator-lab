"""Exercise native server and transaction pooler on a private Unix socket at image build."""
import glob
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import psycopg
from psycopg import pq

assert os.getuid() != 0 and sys.version_info[:2] == (3, 12)
assert psycopg.__version__ == '3.3.8' and pq.__impl__ == 'binary'
pg, = [Path(p) for p in glob.glob('/usr/lib/postgresql/*/bin')]
def quiet(cmd):
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
with tempfile.TemporaryDirectory(prefix='eq17-build-') as path:
    root=Path(path);db=root/'db';sock=root/'pool';sock.mkdir()
    quiet([pg/'initdb','-D',db,'--username=smoke','--auth=trust','--no-locale'])
    quiet([pg/'pg_ctl','-D',db,'-l',root/'pg.log','-o',f"-c listen_addresses='' -c unix_socket_directories='{root}' -p 16544",'-w','start'])
    auth=root/'users';auth.write_text('"smoke" ""\n');cfg=root/'pool.ini'
    cfg.write_text(f"[databases]\npostgres = host={root} port=16544 dbname=postgres\n[pgbouncer]\nlisten_port=16432\nunix_socket_dir={sock}\nauth_type=trust\nauth_file={auth}\npool_mode=transaction\n")
    proc=subprocess.Popen(['/usr/sbin/pgbouncer',cfg],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            if (sock/'.s.PGSQL.16432').exists():break
            if proc.poll() is not None:raise ValueError('pooler failed to start')
            time.sleep(.05)
        with psycopg.connect(host=str(sock),port=16432,user='smoke',dbname='postgres',connect_timeout=2,prepare_threshold=None) as con:
            assert con.execute('SELECT 40+2').fetchone()==(42,)
            print('EQ17_NATIVE_BUILD_SMOKE',pg.parent.name,psycopg.__version__,pq.version(),flush=True)
    finally:
        proc.terminate();proc.wait(timeout=3)
        quiet([pg/'pg_ctl','-D',db,'-m','immediate','-w','stop'])
