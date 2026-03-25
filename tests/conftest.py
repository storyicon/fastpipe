"""Pytest configuration for fastpipe tests.

Adds parent directory to Python path for development without installation.
"""

import sys
from pathlib import Path

# Add parent of fastpipe directory to path
# This allows 'import fastpipe' to work during development
parent_dir = Path(__file__).parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))
