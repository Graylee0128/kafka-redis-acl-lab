#!/bin/sh
# =============================================================================
# 從環境變數產生 Redis ACL 檔再啟動 redis-server。
# =============================================================================
#
# 誰執行：docker-compose.yml 的 redis 服務用 entrypoint 取代官方 image 的啟動指令，
#         本檔以 volume 掛到容器內的 /scripts/entrypoint.sh。
# 何時跑：每次 redis 容器啟動（含 restart），最後 exec 成 redis-server 常駐。
#
# ACL 檔不支援變數，所以在啟動時產生；密碼以 sha256 雜湊（#<hex>）寫入，不落明文。
#
# ACL 檔放在 /data（volume），內建帳號（default / admin / app）每次啟動依 env 重寫，
# 其他帳號（加分題面板用 ACL SETUSER + ACL SAVE 建立的）原樣保留，重啟後仍有效。
#
# 為什麼是 #!/bin/sh 而不是 bash：redis:7.4-alpine 沒有 bash，只有 busybox sh，
# 所以這支腳本只能用 POSIX sh 語法（沒有 [[ ]]、沒有 pipefail）。
# =============================================================================

# -e：任何指令失敗就中止；-u：用到沒定義的變數就中止。
# POSIX sh 沒有 -o pipefail，所以 pipe 中間失敗不會被偵測（下面的 hash 只有一條簡單的 pipe）。
set -eu

# ---------------------------------------------------------------------------
# 必填環境變數
# ---------------------------------------------------------------------------
# ${X:?訊息}：X 沒設定就印出訊息並結束。兩個密碼都來自 .env。
: "${REDIS_ADMIN_PASSWORD:?REDIS_ADMIN_PASSWORD 未設定}"
: "${REDIS_APP_PASSWORD:?REDIS_APP_PASSWORD 未設定}"

# 算出密碼的 SHA-256（64 個十六進位字元），ACL 檔用 #<hex> 的格式存密碼雜湊。
#   printf '%s'：原樣輸出，不加換行。若用 echo 會多一個 \n，被算進雜湊後密碼就對不上。
#   sha256sum 輸出「雜湊值  -」，cut -d' ' -f1 只取第一欄的雜湊值。
hash() { printf '%s' "$1" | sha256sum | cut -d' ' -f1; }

# ACL 檔放在 /data：compose 掛了 volume，ACL SAVE 寫入的帳號重啟後還在。
ACL_FILE=/data/users.acl
# 先寫暫存檔再改名，見下面 mv 的說明
TMP_FILE="$ACL_FILE.tmp"
# 第一次啟動時 volume 是空的，確保目錄存在（-p：已存在不報錯）
mkdir -p /data

# 之後建立的檔案權限是 600，ACL 檔只有擁有者能讀（雖然存的是雜湊，也不該讓人讀）
umask 077

# ---------------------------------------------------------------------------
# 產生 ACL 檔
# ---------------------------------------------------------------------------
# { ...; } > 檔案：把整個區塊的輸出一次寫進暫存檔。
#
# ACL 語法（一行一個帳號）：
#   user <名稱> on|off  啟用 / 停用
#   #<hex>              密碼的 SHA-256 雜湊（檔案不存明文）
#   ~<pattern>          可存取的 key 範圍；~* 是全部
#   &<pattern>          可用的 Pub/Sub 頻道；&* 是全部
#   resetchannels       清空頻道權限 = 不能用 Pub/Sub
#   +@all / -@all       允許 / 拿掉全部指令
#   +<指令>             允許單一指令；client|setinfo 是 CLIENT 指令底下的 SETINFO 子指令
#
# 三個內建帳號：
#   default：Redis 預設帳號。關掉它，不帶帳密就不能連（NOAUTH 測試靠這行）。
#   admin  ：全部 key、全部頻道、全部指令。給 healthcheck、check-order.sh、web 後端建帳號用。
#   app    ：consumer 用。只能碰 myapp:* 的 key，不能用 Pub/Sub，
#            先 -@all 拿掉全部指令，再逐一加回需要的（白名單）：
#              ping、hello、client|setinfo、client|setname → node-redis 連線握手，少了連不上
#              get、set、exists、expire、ttl              → 讀寫 myapp:events:*（最新狀態）與 TTL
#              rpush、lrange                              → 寫入與讀取 myapp:history:*（處理歷程）
#            刻意不給 KEYS、SCAN（盤點全庫、KEYS 會阻塞 Redis）、FLUSHALL、EVAL、CONFIG。
{
  echo "user default off"
  echo "user admin on #$(hash "$REDIS_ADMIN_PASSWORD") ~* &* +@all"
  echo "user app on #$(hash "$REDIS_APP_PASSWORD") ~myapp:* resetchannels -@all +ping +hello +client|setinfo +client|setname +get +set +exists +expire +ttl +rpush +lrange"
  # 已有 ACL 檔（不是第一次啟動）：保留內建三個帳號以外的行，也就是面板建立的帳號。
  #   grep -v  ：反向，只輸出「不符合」的行
  #   -E       ：延伸正規表示式，才能用 (a|b|c)
  # 內建帳號上面已經依目前的 env 重寫過，這樣改了 .env 的密碼會生效，也不會重複。
  # || true：grep 一行都沒輸出時會回傳 1（例如還沒有面板帳號），在 set -e 下會中止腳本，所以補上。
  if [ -f "$ACL_FILE" ]; then
    grep -vE '^user (default|admin|app) ' "$ACL_FILE" || true
  fi
} > "$TMP_FILE"
# mv 在同一個檔案系統上是原子操作：要嘛換成新檔、要嘛維持舊檔。
# 萬一上面寫到一半失敗（set -e 會在 mv 之前就中止），原本的 ACL 檔仍然完整，
# 不會留下寫了一半的檔案，導致 Redis 起不來或面板帳號消失。
mv "$TMP_FILE" "$ACL_FILE"
# 腳本以 root 執行，但 redis-server 會降權成 redis 使用者。
# 不改擁有者的話，Redis 讀不到這個檔（權限 600），ACL SAVE 也寫不回去。
chown redis:redis "$ACL_FILE"

# ---------------------------------------------------------------------------
# 啟動 redis-server
# ---------------------------------------------------------------------------
# 官方 entrypoint 會 chown /data 並降權成 redis 使用者再跑 redis-server
# 我們只是在官方流程前面多做「產生 ACL 檔」這一步，其餘交還給 docker-entrypoint.sh。
#   --aclfile      ：從檔案載入帳號；ACL SAVE 也會寫回這個檔
#   --appendonly yes：開啟 AOF 持久化，每個寫入都記到檔案，重啟後資料還在
# exec：讓 redis-server 取代 sh 成為 PID 1，docker compose stop 的 SIGTERM 才會直接送到 Redis，
#       讓它正常關閉（寫完 AOF）。
exec docker-entrypoint.sh redis-server --aclfile "$ACL_FILE" --appendonly yes
