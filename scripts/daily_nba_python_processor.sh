#!/bin/bash
# DEPRECATED shim -- retained so existing callers keep working.
# The single entrypoint is scripts/daily_nba_data_processor.sh (design D21);
# python is its default language, so this just forwards with -l python.
echo "::warning ::daily_nba_python_processor.sh is deprecated; use scripts/daily_nba_data_processor.sh -l python" >&2
exec bash "$(dirname "$0")/daily_nba_data_processor.sh" "$@" -l python
