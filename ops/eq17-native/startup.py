"""Preserve normal shared-image imports except on the bounded scanner diagnostic."""
import builtins as _builtins
import os as _os
if _os.environ.get('RENDER_SERVICE_ID') == 'srv-d9jm320u01pc73fhincg':
    if not hasattr(_builtins, '_eq17_native_diag_t0_ns'):
        from time import monotonic_ns as _clock
        _builtins._eq17_native_diag_t0_ns = _clock()
    if __name__ == 'app.eq17_bootstrap':
        raise SystemExit(78)
else:
    _directory, _filename = _os.path.split(__file__)
    _original = _os.path.join(_directory, '_shared_' + _filename)
    with open(_original, 'rb') as _source:
        exec(compile(_source.read(), __file__, 'exec'), globals(), globals())
