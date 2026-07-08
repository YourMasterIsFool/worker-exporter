#!/usr/bin/env sh

message_file="$1"

if [ -z "$message_file" ]; then
  exit 0
fi

first_line="$(sed '/^#/d; /^[[:space:]]*$/d; q' "$message_file")"

case "$first_line" in
  ""|Merge\ *|Revert\ *|fixup!\ *|squash!\ *)
    exit 0
    ;;
esac

if printf '%s\n' "$first_line" | grep -Eq '^(feat✨|fix🐛|docs📝|style💄|refactor♻️|perf⚡|test✅|build🏗️|ci👷|chore🔧|revert⏪)\([A-Z0-9_-]+:[A-Z0-9_-]+\): .+ - .+$'; then
  exit 0
fi

cat >&2 <<'EOF'

Format commit message tidak valid.
Gunakan format:
  fix🐛(WORKER:TECHS-001): title - description

Jalankan `git commit` tanpa `-m` untuk memakai prompt otomatis.

EOF
exit 1
