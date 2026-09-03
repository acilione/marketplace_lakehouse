#!/usr/bin/env bash
set -euo pipefail

if git status --short --ignored | grep -E '^!! (data|warehouse|checkpoints|logs|reports|artifacts|benchmark-results)/' >/dev/null; then
  printf '%s\n' "Runtime artifacts are correctly ignored."
fi

if git ls-files | grep -E '\.(docx?|pdf|log|pyc)$|^(data|warehouse|checkpoints|logs|reports|artifacts)/' >/dev/null; then
  printf '%s\n' "Error: a non-source runtime/reference artifact is tracked." >&2
  exit 1
fi

printf '%s\n' "Tracked files are source code, configuration, tests, infrastructure, or documentation."
