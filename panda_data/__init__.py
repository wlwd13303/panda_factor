"""
Top-level proxy module for the panda_data package.

This makes `import panda_data` work when running from the repo root by
re-exporting the real implementation from `panda_data/panda_data`.
"""

# Re-export all public symbols from the inner package
from .panda_data import *  # noqa: F401,F403

# Ensure __all__ is propagated if defined in the inner package
try:
    from .panda_data import __all__ as __all__  # type: ignore
except Exception:
    # Fallback: don't define __all__ if inner package doesn't provide it
    pass


