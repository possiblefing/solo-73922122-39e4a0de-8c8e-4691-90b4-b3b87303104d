#!/usr/bin/env bash
# 一键演示“坏行后接好行”：
#   1. 向 nginx 的 JSON 访问日志注入 1 行非法 UTF-8 + 2 行合法请求
#   2. 等待采集器轮询
#   3. 打印 /health、/events、/malformed 的关键结果
#
# 前置：docker compose up -d 已启动
# 用法: ./scripts/demo-malformed.sh
set -euo pipefail
cd "$(dirname "$0")/.."

API="${API:-http://localhost:8000}"

echo "== 1. 注入坏行(0xFF 0xFE) + 其后两行好行 =="
docker compose exec -T nginx /usr/local/bin/inject-demo-lines.sh

echo
echo "== 2. 触发一次采集并等待 =="
curl -fsS -X POST "$API/ingest/poll-now" >/dev/null || true
sleep 1
curl -fsS -X POST "$API/ingest/poll-now" >/dev/null || true

echo
echo "== 3. /health 计数（events 与 malformed 分开） =="
curl -fsS "$API/health" | python3 -m json.tool

echo
echo "== 4. /events 中只有好行（坏行绝不出现） =="
curl -fsS "$API/events?path=/demo/after-bad-line&limit=10" | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(f"total={d[\"total\"]}")
for i in d["items"]:
    print(f"  id={i[\"id\"]} status={i[\"status\"]} path={i[\"path\"]} "
          f"src={i[\"source_code\"]} @{i[\"byte_offset\"]}")
'
echo "-- 坏路径若混入会命中下面这条（应为 total=0）："
curl -fsS "$API/events?path=%2Fdemo%2Fbroken%2F%EF%BF%BD%EF%BF%BD&limit=10" \
  | python3 -c 'import json,sys; print("  total =", json.load(sys.stdin)["total"])'

echo
echo "== 5. /malformed：编码错误行，Base64 无损 =="
curl -fsS "$API/malformed?error_kind=invalid_utf8&limit=5" | python3 -c '
import base64, json, sys
d = json.load(sys.stdin)
print(f"total={d[\"total\"]} by_kind={d[\"by_error_kind\"]}")
for i in d["items"]:
    raw = base64.b64decode(i["raw_line_base64"])
    print(f"  id={i[\"id\"]} kind={i[\"error_kind\"]} src={i[\"source_code\"]} "
          f"dev={i[\"dev\"]} inode={i[\"inode\"]} @{i[\"byte_offset\"]} "
          f"lossless={i[\"raw_line_lossless\"]} bytes={i[\"raw_line_bytes_length\"]}")
    print(f"  raw(cat -v): {raw!r}")
    print(f"  has 0xff/0xfe: {b\"\\xff\\xfe\" in raw}")
'

echo
echo "完成。也可打开 $API/docs 在 Swagger 中直接试 /events 与 /malformed。"
