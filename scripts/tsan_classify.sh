#!/usr/bin/env bash
# Summarises ThreadSanitizer race reports by where each racing access really
# happens: the first stack frame that is not the C++ standard library or the
# sanitizer runtime. Used to tell races in our code (frames in src/) from
# races inside gRPC, which is not built with TSan (see DESIGN.md).
#
# Usage: scripts/tsan_classify.sh TSAN_OUTPUT_FILE
set -euo pipefail

file=${1:?usage: $0 TSAN_OUTPUT_FILE}
echo "reports: $(grep -c 'WARNING: ThreadSanitizer' "$file" || true)"
awk '
  /^  (Write|Read|Atomic|Previous)/ { want = 1; next }
  want && /#[0-9]+ / {
    if ($0 ~ /std::|__cxx11|libsanitizer|sanitizer_common|basic_string|char_traits/) next
    line = $0
    sub(/ \(BuildId.*/, "", line)
    sub(/^ +#[0-9]+ /, "", line)
    print line
    want = 0
  }
' "$file" | sed -E 's/\+0x[0-9a-f]+//; s/\(server_tests\)//' | cut -c1-140 | sort | uniq -c | sort -rn
