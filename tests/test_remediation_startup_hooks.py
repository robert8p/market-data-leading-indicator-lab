"""Test startup boundaries with finite imports; never start a real lane."""
from pathlib import Path
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
FLAGS = ('INDEX_FUTURES_INGEST_ENABLED', 'OPTION_CONVEXITY_HTTP_ENABLED',
         'CINT001_BOOKTICKER_ENABLED', 'PHASE3_FORWARD_MONITOR_ENABLED')


def execute_hook(root, relative, env, patch_failure=False):
    env = dict(env)
    events = []
    def note(name):
        return lambda *args, **kwargs: events.append(name)
    def patch():
        events.append('phase3_patch')
        if patch_failure:
            raise ValueError('synthetic patch failure')
    def settings():
        events.append('settings_read')
        return SimpleNamespace(supabase_service_role_key='synthetic-fixture-only')
    logger = SimpleNamespace(info=note('info'), error=note('error'), exception=note('exception'))
    modules = {
        '__future__': SimpleNamespace(annotations=None),
        'os': SimpleNamespace(environ=env, getenv=env.get),
        'logging': SimpleNamespace(getLogger=lambda *args: logger),
        'urllib.parse': SimpleNamespace(unquote=lambda s:s, urlsplit=lambda s:SimpleNamespace(password=None)),
        'pathlib': SimpleNamespace(Path=Path),
        'typing': SimpleNamespace(Any=object),
        'app': SimpleNamespace(config=SimpleNamespace(get_settings=settings)),
        'app.index_futures_fixed_window_bootstrap': SimpleNamespace(start_fixed_window_index_futures_ingestion=note('index_start')),
        'option_convexity_http_worker': SimpleNamespace(start_background=note('options_start')),
        'app.cint001_bookticker': SimpleNamespace(start_background=note('bookticker_start')),
        'app.phase3_gateway_patch': SimpleNamespace(install_gateway_patch=patch),
        'app.phase3_forward': SimpleNamespace(start_background=note('phase3_start')),
    }
    class UnexpectedImport(BaseException):
        pass
    def restricted_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level or name not in modules:
            raise UnexpectedImport('Unexpected startup import: ' + name)
        events.append('import:' + name)
        return modules[name]
    allowed = dict(__import__=restricted_import, bool=bool, str=str, getattr=getattr,
                   callable=callable, Exception=Exception, ValueError=ValueError, RuntimeError=RuntimeError)
    def run(path, run_name='synthetic_hook'):
        path = Path(path)
        assert path.resolve().is_relative_to(root.resolve())
        ns = {'__name__': run_name, '__file__': str(path), '__builtins__': allowed}
        exec(compile(path.read_bytes(), str(path), 'exec'), ns)
        return ns
    modules['runpy'] = SimpleNamespace(run_path=run)
    run(root/relative)
    return events, env


class RemediationStartupHookTests(unittest.TestCase):
    def test_both_hooks_block_all_legacy_optins_before_import_or_credential_read(self):
        for relative in ('sitecustomize.py', 'phase3_bootstrap/sitecustomize.py'):
            for remediation in ('true', 'TRUE'):
                for auth in (False, True):
                    env = dict.fromkeys(FLAGS, 'true')
                    env['MARKET_DATA_REMEDIATION_ENABLED'] = remediation
                    if auth:
                        env['PHASE3_FORWARD_GATEWAY_TOKEN'] = 'synthetic-fixture-only'
                    with self.subTest(hook=relative, remediation=remediation, auth=auth):
                        events, after = execute_hook(ROOT, relative, env)
                        forbidden = [event for event in events if event.startswith('import:app') or
                                     event == 'import:option_convexity_http_worker' or
                                     event.endswith('_start') or event in ('phase3_patch','settings_read','error','exception')]
                        self.assertEqual(forbidden, [])
                        self.assertEqual(after, env)

    def test_root_legacy_optins_preserve_start_and_patch_order(self):
        events, after = execute_hook(ROOT, 'sitecustomize.py', {
            **dict.fromkeys(FLAGS, 'true'), 'PHASE3_FORWARD_GATEWAY_TOKEN':'synthetic-fixture-only'})
        self.assertEqual([e for e in events if e.endswith('_start') or e == 'phase3_patch'],
                         ['index_start','options_start','bookticker_start','phase3_patch','phase3_start'])
        self.assertEqual(after['PHASE3_FORWARD_MONITOR_ENABLED'], 'true')
        self.assertNotIn('exception', events)

    def test_flags_off_remain_inert(self):
        events, _ = execute_hook(ROOT, 'sitecustomize.py', {})
        self.assertFalse(any(e.startswith('import:app') or e.endswith('_start') for e in events))

    def test_missing_authentication_still_fails_closed(self):
        events, after = execute_hook(ROOT, 'sitecustomize.py', {'PHASE3_FORWARD_MONITOR_ENABLED':'true'})
        self.assertIn('error', events)
        self.assertNotIn('phase3_patch', events)
        self.assertNotIn('phase3_start', events)
        self.assertEqual(after['PHASE3_FORWARD_MONITOR_ENABLED'], 'false')

    def test_failed_patch_still_prevents_collector_start(self):
        events, after = execute_hook(ROOT, 'sitecustomize.py', {
            'PHASE3_FORWARD_MONITOR_ENABLED':'true', 'PHASE3_FORWARD_GATEWAY_TOKEN':'synthetic-fixture-only'}, True)
        self.assertIn('phase3_patch', events)
        self.assertIn('exception', events)
        self.assertNotIn('phase3_start', events)
        self.assertEqual(after['PHASE3_FORWARD_MONITOR_ENABLED'], 'false')

    def test_alternate_hook_preserves_synthetic_settings_preload_outside_remediation(self):
        events, after = execute_hook(ROOT, 'phase3_bootstrap/sitecustomize.py', {'PHASE3_FORWARD_MONITOR_ENABLED':'true'})
        self.assertEqual([e for e in events if e in ('settings_read','phase3_patch','phase3_start')],
                         ['settings_read','phase3_patch','phase3_start'])
        self.assertEqual(after['SUPABASE_SERVICE_ROLE_KEY'], 'synthetic-fixture-only')


if __name__ == '__main__':
    unittest.main()
