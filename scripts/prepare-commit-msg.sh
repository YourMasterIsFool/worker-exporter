#!/usr/bin/env sh

message_file="$1"
source="$2"

case "$source" in
  message|merge|squash|commit)
    exit 0
    ;;
esac

if [ -z "$message_file" ] || [ ! -t 0 ]; then
  exit 0
fi

normalize_scope_part() {
  printf '%s' "$1" \
    | tr '[:lower:]' '[:upper:]' \
    | sed -E 's/[[:space:]]+/-/g; s/[^A-Z0-9_-]//g'
}

ask() {
  printf '%s' "$1"
  IFS= read -r answer
  printf '%s' "$answer"
}

ask_required() {
  prompt="$1"
  value=""

  while [ -z "$value" ]; do
    value="$(ask "$prompt")"
  done

  printf '%s' "$value"
}

ask_scope_part() {
  prompt="$1"
  default_value="$2"
  value=""

  while [ -z "$value" ]; do
    answer="$(ask "$prompt")"
    if [ -z "$answer" ] && [ -n "$default_value" ]; then
      answer="$default_value"
    fi
    value="$(normalize_scope_part "$answer")"
  done

  printf '%s' "$value"
}

print_conventional_commit_menu() {
  printf '\nConventional Commits:\n'
  printf '  1. fix      - Bug fix\n'
  printf '  2. feat     - Feature\n'
  printf '  3. docs     - Documentation\n'
  printf '  4. style    - Style\n'
  printf '  5. refactor - Refactor\n'
  printf '  6. perf     - Performance\n'
  printf '  7. test     - Test\n'
  printf '  8. build    - Build\n'
  printf '  9. ci       - CI\n'
  printf ' 10. chore    - Chore\n'
  printf ' 11. revert   - Revert\n'
}

ask_conventional_commit() {
  commit_type=""
  icon=""

  while [ -z "$commit_type" ]; do
    print_conventional_commit_menu
    commit_choice="$(ask 'Pilih Conventional Commit (default 1/fix): ')"

    case "$commit_choice" in
      ""|1|fix) commit_type="fix"; icon="🐛" ;;
      2|feat) commit_type="feat"; icon="✨" ;;
      3|docs) commit_type="docs"; icon="📝" ;;
      4|style) commit_type="style"; icon="💄" ;;
      5|refactor) commit_type="refactor"; icon="♻️" ;;
      6|perf) commit_type="perf"; icon="⚡" ;;
      7|test) commit_type="test"; icon="✅" ;;
      8|build) commit_type="build"; icon="🏗️" ;;
      9|ci) commit_type="ci"; icon="👷" ;;
      10|chore) commit_type="chore"; icon="🔧" ;;
      11|revert) commit_type="revert"; icon="⏪" ;;
      *) printf 'Pilihan tidak valid. Pilih nomor 1-11 atau nama commit type.\n' ;;
    esac
  done
}

ask_conventional_commit
domain="$(ask_scope_part 'Nama Domain: (default WORKER) ' 'WORKER')"
ticket="$(ask_scope_part 'Nama Ticket(TECHS-001): ' '')"
title="$(ask_required 'title: ')"
description="$(ask_required 'Description: ')"
message="${commit_type}${icon}(${domain}:${ticket}): ${title} - ${description}"

printf '%s\n' "$message" > "$message_file"
printf '\nCommit message:\n%s\n\n' "$message"
