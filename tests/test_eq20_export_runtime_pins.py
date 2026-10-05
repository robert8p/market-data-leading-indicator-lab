"""Test the actual supervisor and constructor after exact image pin bindings."""
import ast
import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('eq20_pin_builder_test', ROOT / 'scripts/eq20_export_runtime_pins.py')
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


class ExportPinTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = (ROOT / 'app/eq20_source_supervisor.py').read_bytes()
        cls.updated = builder.patched_bytes(cls.original)
        cls.constructor_original = (ROOT / 'app/eq20_full_population_source.py').read_bytes()
        cls.constructor_updated = builder.patched_constructor_bytes(cls.constructor_original)
        cls.temp = tempfile.TemporaryDirectory()
        path = Path(cls.temp.name) / 'eq20_source_supervisor.py'
        path.write_bytes(cls.updated)
        spec = importlib.util.spec_from_file_location('eq20_export_image_test', path)
        cls.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)
        constructor_path = Path(cls.temp.name) / 'eq20_full_population_source.py'
        constructor_path.write_bytes(cls.constructor_updated)
        spec = importlib.util.spec_from_file_location('eq20_constructor_image_test', constructor_path)
        cls.constructor = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.constructor)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_exact_original_and_image_hashes(self):
        self.assertEqual(hashlib.sha256(self.original).hexdigest(), builder.BASE_SHA256)
        self.assertEqual(hashlib.sha256(self.updated).hexdigest(), builder.IMAGE_SHA256)
        self.assertEqual(hashlib.sha256(self.constructor_original).hexdigest(), builder.CONSTRUCTOR_BASE_SHA256)
        self.assertEqual(hashlib.sha256(self.constructor_updated).hexdigest(), builder.CONSTRUCTOR_IMAGE_SHA256)

    def test_constructor_changes_only_exact_guard_pin(self):
        original = self.constructor_original.decode()
        updated = self.constructor_updated.decode()
        old = "PROCESS_GUARDS_SHA256 = '" + builder.BASE_SHA256 + "'"
        new = "PROCESS_GUARDS_SHA256 = '" + builder.IMAGE_SHA256 + "'"
        self.assertEqual(original.count(old), 1)
        self.assertEqual(updated.count(new), 1)
        self.assertEqual(updated.replace(new, old, 1), original)

    def test_actual_constructor_accepts_composed_guard_image(self):
        guards = self.constructor.source_guards()
        self.assertEqual(hashlib.sha256(Path(guards.__file__).read_bytes()).hexdigest(), builder.IMAGE_SHA256)
        self.assertEqual(guards.PRIVATE_BUNDLE_SHA256, builder.NEW_BUNDLE)
        self.assertEqual((guards.RESERVED_SECONDS, guards.CHILD_SECONDS, guards.PARENT_SECONDS, guards.TERMINAL_SECONDS), (30, 18, 6, 6))
        self.assertEqual(guards.MAX_SCRATCH_BYTES, 2147483648)

    def test_constructor_rejects_untransformed_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'eq20_full_population_source.py'
            path.write_bytes(self.constructor_updated)
            path.with_name('eq20_source_supervisor.py').write_bytes(self.original)
            spec = importlib.util.spec_from_file_location('eq20_constructor_wrong_image_test', path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            with self.assertRaisesRegex(module.SourceGateClosed, 'SOURCE_REVIEWED_PROCESS_GUARDS_PIN_REQUIRED'):
                module.source_guards()

    def test_only_two_assignments_change(self):
        def without_pins(raw):
            tree = ast.parse(raw)
            tree.body = [node for node in tree.body if not isinstance(node, ast.Assign) or not any(isinstance(t, ast.Name) and t.id in ('PRIVATE_BUNDLE_SHA256', 'PRIVATE_FILES') for t in node.targets)]
            return ast.dump(tree)
        self.assertEqual(without_pins(self.original), without_pins(self.updated))

    def test_unchanged_resource_guards(self):
        m = self.module
        self.assertEqual(m.MAX_CHILD_RSS_BYTES, 268435456)
        self.assertEqual(m.MAX_SCRATCH_BYTES, 2147483648)
        self.assertEqual((m.RESERVED_SECONDS, m.CHILD_SECONDS, m.PARENT_SECONDS, m.TERMINAL_SECONDS), (30, 18, 6, 6))

    def test_actual_bundle_admission_accepts_exact_new_bytes(self):
        m = self.module
        poll = dict(protocol_version=m.PROTOCOL, entrypoint=m.ENTRYPOINT, bundle_sha256=builder.NEW_BUNDLE,
                    files=[dict(name=n, bytes=b, sha256=h) for n,(b,h) in m.PRIVATE_FILES.items()])
        m.validate_pins(poll)
        self.assertEqual(m.PRIVATE_FILES['w10_source_worker_v2.py'], (100279, 'c0e97c6988fb92f17870b285deefa1cdf43bcdde6ceca98e19a33a330dc3bb25'))

    def test_different_private_bytes_rejected(self):
        m = self.module
        poll = dict(protocol_version=m.PROTOCOL, entrypoint=m.ENTRYPOINT, bundle_sha256=builder.NEW_BUNDLE,
                    files=[dict(name=n, bytes=b, sha256=h) for n,(b,h) in m.PRIVATE_FILES.items()])
        poll['files'][-1]['sha256'] = '0' * 64
        with self.assertRaises(m.GuardError):
            m.validate_pins(poll)

    def test_unknown_predecessor_rejected(self):
        with self.assertRaises(ValueError):
            builder.patched_bytes(self.original + b'\n')

    def test_double_application_rejected(self):
        with self.assertRaises(ValueError):
            builder.patched_bytes(self.updated)

    def test_unknown_constructor_or_double_binding_rejected(self):
        for raw in (self.constructor_original + b'\n', self.constructor_updated):
            with self.subTest(sha256=hashlib.sha256(raw).hexdigest()), self.assertRaises(ValueError):
                builder.patched_constructor_bytes(raw)
