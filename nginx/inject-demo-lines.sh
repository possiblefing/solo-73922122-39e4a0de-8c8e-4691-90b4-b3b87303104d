#!/bin/sh
# 演示用：向正在采集的 JSON 访问日志追加
#   1) 一行含有非法 UTF-8 字节（0xFF 0xFE）的坏行，
#   2) 随后两行完全合法的请求行。
# 采集器应把坏行隔离进 /malformed（Base64 无损），而两行好行照常进入
# /events —— 用来通过 /docs 验证“坏行后接好行”的连续性。
#
# 用法（nginx 容器内）:
#   /usr/local/bin/inject-demo-lines.sh
# 或宿主机:
#   docker compose exec nginx /usr/local/bin/inject-demo-lines.sh
#
# 可移植性说明：\377 / \376 直接写在 printf 的 *格式串* 里。POSIX 规定
# printf 会解释格式串中的 \ddd 八进制转义并输出原始字节；dash、bash、
# busybox ash（nginx:alpine 的 /bin/sh）都一致。若把转义放进变量再用 %s
# 输出，反斜杠会原样写出，得不到坏字节——所以不要那样改。
set -eu

LOG_FILE="${LOG_FILE:-/var/log/nginx/json/access.log}"
TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

{
    # 坏行：路径字段里就是原始 0xFF 0xFE 两个非法字节
    printf '{"time":"%s","remote_addr":"127.0.0.1","method":"GET","uri":"/demo/broken/\377\376","path":"/demo/broken/\377\376","status":500,"bytes_sent":0,"request_time":0.001,"referer":"","user_agent":"demo-injector","request":"GET /demo/broken/ HTTP/1.1"}\n' "$TS"

    # 紧随其后的两行合法请求（证明采集没有在坏行处停下）
    printf '{"time":"%s","remote_addr":"127.0.0.1","method":"GET","uri":"/demo/after-bad-line","path":"/demo/after-bad-line","status":503,"bytes_sent":0,"request_time":0.002,"referer":"","user_agent":"demo-injector","request":"GET /demo/after-bad-line HTTP/1.1"}\n' "$TS"
    printf '{"time":"%s","remote_addr":"127.0.0.1","method":"GET","uri":"/demo/after-bad-line","path":"/demo/after-bad-line","status":200,"bytes_sent":2,"request_time":0.001,"referer":"","user_agent":"demo-injector","request":"GET /demo/after-bad-line HTTP/1.1"}\n' "$TS"
} >> "$LOG_FILE"

echo "injected: 1 invalid-UTF-8 line + 2 valid lines -> $LOG_FILE"
