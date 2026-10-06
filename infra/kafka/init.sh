#!/usr/bin/env bash
# =============================================================================
# Kafka 一次性初始化：建立 client 帳號、topic、ACL。全部冪等，重跑不會壞。
# =============================================================================
#
# 誰執行：docker-compose.yml 的 kafka-init 服務（用同一個 apache/kafka image，裡面有命令列工具），
#         本檔以 volume 掛到容器內的 /scripts/init.sh。
# 何時跑：kafka 變成 healthy 之後（depends_on: condition: service_healthy），跑完就結束。
#         restart: "no"，不會被重啟；成功時容器狀態是 Exited (0)。
#         producer / consumer / web 都等它成功結束（service_completed_successfully）才啟動。
# 做什麼：
#   [1/3] 建立 SCRAM 帳號 producer、consumer
#   [2/3] 建立 topic（預設 orders，3 個 partition）
#   [3/3] 設定 ACL：producer 只能寫、consumer 只能讀且只能用指定的 group
#   最後印出 topic 與 ACL，docker compose logs kafka-init 一眼看到初始化結果。
#
# 冪等（重跑結果一樣、不會出錯）：
#   帳號用 --alter 覆寫；topic 有 --if-not-exists；ACL 重複加入只是同一條規則。
#
# admin 帳號不在這裡建：它在 start.sh format 時就寫進 metadata（broker 啟動要用）。
# =============================================================================

# -e 失敗就停、-u 未定義變數就停、-o pipefail pipe 任一段失敗就算失敗
set -euo pipefail

# ---------------------------------------------------------------------------
# 參數
# ---------------------------------------------------------------------------
# ${X:?}  ：X 沒設定就報錯並結束（必填）。三個密碼都來自 .env。
: "${KAFKA_ADMIN_PASSWORD:?}"
: "${KAFKA_PRODUCER_PASSWORD:?}"
: "${KAFKA_CONSUMER_PASSWORD:?}"
# ${X:-預設}：X 沒設定就用預設值（選填）。docker-compose.yml 有明確傳入，這裡的預設值是保險。
# 注意 :? 與 :- 的差別：一個是「沒有就報錯」，一個是「沒有就用預設」。
TOPIC="${KAFKA_TOPIC:-orders}"
GROUP="${KAFKA_CONSUMER_GROUP:-order-consumers}"
# partition 數 = 能平行處理的上限。3 個讓 2 個 consumer 都分得到工作（2:1）。
# 之後只能增加不能減少，而且增加會改變 key → partition 的對應。
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"

# ---------------------------------------------------------------------------
# 密碼格式檢查
# ---------------------------------------------------------------------------
# 密碼會嵌進 SCRAM-SHA-512=[password=...] 字串，含逗號或中括號會破壞語法，所以限英數字 12 碼以上。
# ${!name} 是 bash 的「間接引用」：name 的值是變數名稱字串（例如 KAFKA_PRODUCER_PASSWORD），
# ${!name} 取的是「那個變數的值」。這樣一個迴圈就能檢查兩個密碼，錯誤訊息也能印出是哪一個。
for name in KAFKA_PRODUCER_PASSWORD KAFKA_CONSUMER_PASSWORD; do
  if [[ ! "${!name}" =~ ^[A-Za-z0-9]{12,}$ ]]; then
    echo "$name 需為 12 碼以上英數字" >&2
    exit 1
  fi
done

# Kafka 命令列工具所在目錄
BIN=/opt/kafka/bin
# bootstrap server：走 INTERNAL listener（kafka-init 在 compose 網路內，認得 kafka 這個名稱）
BS=kafka:9092
# admin 的 client 設定檔。start.sh 也寫了一份，但那是在 kafka 容器；
# 不同容器的 /tmp 不共用，所以這裡要再寫一次。
CFG=/tmp/admin.properties

# 之後建立的檔案權限是 600，因為設定檔含 admin 密碼
umask 077
# heredoc（EOF 沒加引號，所以內容中的變數會被展開）：
#   以 admin 身分、SCRAM-SHA-512、不加密（SASL_PLAINTEXT）連線
cat > "$CFG" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}";
EOF

# ---------------------------------------------------------------------------
# [1/3] 建立 SCRAM 帳號
# ---------------------------------------------------------------------------
# kafka-configs.sh --alter --entity-type users：修改「使用者」這類實體的設定
#   --entity-name    ：帳號名稱
#   --add-config     ：加上 SCRAM-SHA-512 憑證（broker 端只存加鹽雜湊，不存明文）
# 以 admin 身分（--command-config）透過 Admin API 寫進 KRaft metadata，broker 不用重啟。
# 這就是選 SCRAM 而不選 PLAIN 的原因：PLAIN 的帳號寫死在設定檔，加帳號要重啟。
# 加分題的面板建帳號，也是呼叫同一支工具（web/src/kafka-admin.js）。
echo "==> [1/3] 建立 SCRAM 帳號 producer / consumer（執行期新增，不需重啟 broker）"
"$BIN/kafka-configs.sh" --bootstrap-server "$BS" --command-config "$CFG" \
  --alter --entity-type users --entity-name producer \
  --add-config "SCRAM-SHA-512=[password=${KAFKA_PRODUCER_PASSWORD}]"
"$BIN/kafka-configs.sh" --bootstrap-server "$BS" --command-config "$CFG" \
  --alter --entity-type users --entity-name consumer \
  --add-config "SCRAM-SHA-512=[password=${KAFKA_CONSUMER_PASSWORD}]"

# ---------------------------------------------------------------------------
# [2/3] 建立 topic
# ---------------------------------------------------------------------------
# start.sh 設了 auto.create.topics.enable=false，所以 topic 一定要在這裡明確建立。
#   --if-not-exists       ：已存在就跳過，不報錯（冪等）
#   --replication-factor 1：單節點只能放 1 份副本
echo "==> [2/3] 建立 topic ${TOPIC}（${PARTITIONS} partitions）"
"$BIN/kafka-topics.sh" --bootstrap-server "$BS" --command-config "$CFG" \
  --create --if-not-exists --topic "$TOPIC" \
  --partitions "$PARTITIONS" --replication-factor 1

# ---------------------------------------------------------------------------
# [3/3] 設定 ACL（最小權限）
# ---------------------------------------------------------------------------
# start.sh 設了 allow.everyone.if.no.acl.found=false（預設拒絕），
# 所以下面沒列出來的權限都沒有。每條 ACL 的意思是：
#   允許 (--allow-principal) 某帳號 對 某資源 (--topic / --group) 做 某操作 (--operation)
#
#   帳號      資源                     操作              為什麼
#   producer  topic orders             Write, Describe   寫訊息；Describe 查 partition / leader metadata，連線必需
#   consumer  topic orders             Read, Describe    讀訊息
#   consumer  group order-consumers    Read              加入 group、commit offset；只有這個 group
#
# consumer 只能用 order-consumers 這個 group，所以不能另開 group 從頭重讀全部資料。
# producer 沒有任何 Read，被攻破也讀不到資料；consumer 沒有任何 Write，寫入在
# idempotent producer 初始化時就會被 ClusterAuthorizationException 擋下。
echo "==> [3/3] 設定 ACL（最小權限）"
# producer：只能寫 topic（Describe 是取 metadata 必需）
"$BIN/kafka-acls.sh" --bootstrap-server "$BS" --command-config "$CFG" \
  --add --allow-principal User:producer \
  --operation Write --operation Describe --topic "$TOPIC"
# consumer：只能讀 topic，且只能加入指定的 group
"$BIN/kafka-acls.sh" --bootstrap-server "$BS" --command-config "$CFG" \
  --add --allow-principal User:consumer \
  --operation Read --operation Describe --topic "$TOPIC"
"$BIN/kafka-acls.sh" --bootstrap-server "$BS" --command-config "$CFG" \
  --add --allow-principal User:consumer \
  --operation Read --group "$GROUP"

# ---------------------------------------------------------------------------
# 印出結果
# ---------------------------------------------------------------------------
# --describe：topic 的 partition 數、每個 partition 的 leader 與副本
# --list    ：目前所有 ACL
# 用 docker compose logs kafka-init 就能確認初始化結果，不用另外進容器查。
echo "==> 完成。目前 topic 與 ACL："
"$BIN/kafka-topics.sh" --bootstrap-server "$BS" --command-config "$CFG" --describe --topic "$TOPIC"
"$BIN/kafka-acls.sh" --bootstrap-server "$BS" --command-config "$CFG" --list
