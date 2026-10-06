#!/usr/bin/env bash
# =============================================================================
# 從 Redis 的 myapp:history:* 驗證步驟 3：
#   1. 每筆訂單的 seq 是否從 1 連續遞增（順序性）
#   2. 每筆訂單是否只落在一個 partition（相同 key → 相同 partition）
#   3. 每個 partition 由哪個 consumer 處理、各處理幾筆（負載平衡）
# 用 Redis admin 帳號讀取（app 帳號沒有 SCAN 權限，這是刻意的）。違規時 exit 1。
# =============================================================================
#
# 用法：bash scripts/check-order.sh（在哪個目錄執行都可以，見下面的 cd）
#
# 資料來源：consumer 每處理一筆事件，就 RPUSH 一筆 JSON 到 myapp:history:<order_id>，
# 內容是原始事件加上 handled_by（consumer hostname）、partition、offset。
# 一張訂單的 LIST 依處理順序排列，所以只要逐筆比對就能檢查順序與 partition。
#
# 輸出範例：
#   orders=<N> events=<M> seq_not_contiguous=0 multi_partition=0
#   partition -> consumer: events
#     P0 -> <consumer-A>: ...
#     P1 -> <consumer-A>: ...
#     P2 -> <consumer-B>: ...
#
# 流程：Redis 容器內的 sh 迴圈把資料印成文字 → pipe 給 host 上的 awk 統計與判斷。
# =============================================================================

# -e 失敗就停、-u 未定義變數就停、-o pipefail pipe 任一段失敗就算失敗
# （例如 docker compose exec 失敗，不會因為 awk 成功就被當成通過）
set -euo pipefail
# 切到 lab 根目錄（本檔所在目錄的上一層），docker compose 才找得到 docker-compose.yml。
#   $0 是腳本路徑，dirname 取目錄部分，/.. 是上一層。
cd "$(dirname "$0")/.."

# ---------------------------------------------------------------------------
# 第 1 段：在 Redis 容器內列出所有訂單歷程
# ---------------------------------------------------------------------------
# docker compose exec -T redis sh -c '...'：在 redis 容器裡執行一段 sh 腳本。
#   -T：不配置 TTY。輸出要 pipe 給 awk，配置 TTY 會混入 \r 等控制字元。
#   < /dev/null：exec 不從 stdin 讀東西，避免它吃掉呼叫端的輸入。
# 容器內已設定 REDISCLI_AUTH（admin 密碼），所以 redis-cli 不用帶 --pass，密碼不會出現在指令列。
#
# 輸出格式（awk 依此解析）：
#   K myapp:history:<order_id>      ← 每張訂單的標頭行，以「K 」開頭
#   {"order_id":...,"seq":1,...}    ← 該訂單的每一筆處理紀錄，依處理順序
#   {"order_id":...,"seq":2,...}
docker compose exec -T redis sh -c '
  # --scan --pattern：用 SCAN 逐批列出符合的 key，不會像 KEYS 一樣一次掃全庫、阻塞 Redis
  # （所以要用 admin；app 帳號刻意沒有 SCAN 權限）
  for k in $(redis-cli --no-auth-warning --user admin --scan --pattern "myapp:history:*"); do
    echo "K $k"
    # LRANGE key 0 -1：取出 LIST 的全部元素（-1 代表最後一個）
    redis-cli --no-auth-warning --user admin lrange "$k" 0 -1
  done' </dev/null | awk '
  # -------------------------------------------------------------------------
  # 第 2 段：在 host 上用 awk 逐行統計
  # -------------------------------------------------------------------------
  # awk 對每一行輸入，依序比對「條件 { 動作 }」。用到的陣列：
  #   part[訂單]    該訂單第一次出現的 partition
  #   bad[訂單]     seq 不連續的訂單
  #   multi[訂單]   出現在超過一個 partition 的訂單
  #   load["P? -> consumer"]  每個 partition 與 consumer 組合處理了幾筆

  # 標頭行：記下目前是哪張訂單，並把這張訂單的筆數歸零；next 跳到下一行，不往下執行
  /^K / { key = $2; n = 0; next }

  # 其他行都是一筆 JSON 處理紀錄
  {
    # 不引入 JSON 解析器，用 regex 取欄位值：
    #   match() 找到時設定 RSTART（起點）與 RLENGTH（長度），substr() 再切掉欄位名稱的部分。
    #   數字是欄位名稱的字元數，例如 "seq": 是 6 個字元。
    #   seq 最後的 + 0 把字串轉成數字，才能跟 n 比大小。
    #   handled_by 多減 1，是為了去掉值結尾的那個雙引號。
    match($0, /"seq":[0-9]+/);            seq = substr($0, RSTART + 6,  RLENGTH - 6) + 0
    match($0, /"partition":[0-9]+/);      p   = substr($0, RSTART + 12, RLENGTH - 12)
    match($0, /"handled_by":"[^"]+"/);    c   = substr($0, RSTART + 14, RLENGTH - 15)
    # n：這是該訂單的第幾筆紀錄
    n++
    # 檢查 1（順序性）：第 n 筆的 seq 應該剛好是 n，也就是 1、2、3… 連續。
    # 亂序、漏掉或重複寫入都會讓兩者對不上。
    if (seq != n) bad[key] = 1
    # 檢查 2（同 key 同 partition）：這張訂單之前出現過，且 partition 跟之前不同 → 違規
    if (key in part && part[key] != p) multi[key] = 1
    part[key] = p
    # 檢查 3（負載平衡）：累計每個「partition → consumer」組合處理的筆數
    load["P" p " -> " c]++
    # 事件總數
    total++
  }

  # 全部讀完後輸出結果
  END {
    # awk 陣列沒有長度函式（POSIX），用迴圈數 key 的個數：
    #   orders = 訂單數；b = seq 不連續的訂單數；m = 跨 partition 的訂單數
    orders = 0; for (k in part) orders++
    b = 0; for (k in bad) b++
    m = 0; for (k in multi) m++
    printf "orders=%d events=%d seq_not_contiguous=%d multi_partition=%d\n", orders, total, b, m
    print "partition -> consumer: events"
    # for (x in 陣列) 的順序不固定，所以 P0 / P1 / P2 的列出順序每次可能不同
    for (x in load) printf "  %s: %d\n", x, load[x]
    # 任一檢查違規就 exit 1；配合 pipefail，整支腳本的 exit code 也會是 1，可以接在 CI 或 && 後面判斷
    exit (b > 0 || m > 0) ? 1 : 0
  }'
