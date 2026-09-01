"""Runtime overlay for compatibility fixes.

The package path is extended so unchanged modules continue to load from the
integrity-checked source bundle while selected modules can be safely replaced.
"""
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)
