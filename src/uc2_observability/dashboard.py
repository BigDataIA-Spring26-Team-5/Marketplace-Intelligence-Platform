"""
UC2 Observability Layer — Dashboard entry point.

The full Streamlit implementation lives in streamlit_app.py.
This module re-exports `main` for backward compatibility and provides
the __main__ entry point.
"""

from .streamlit_app import main

if __name__ == "__main__":
    main()
