#!/usr/bin/env bash
# =============================================================================
# Kafka 單節點 KRaft（broker + controller 同一個 process）啟動腳本
# =============================================================================
#
# 誰執行：docker-compose.yml 的 kafka 服務用 entrypoint 取代官方 image 的啟動指令，
#         本檔以 volume 掛到容器內的 /scripts/start.sh。
# 何時跑：每次 kafka 容器啟動（含 restart），常駐不結束。
# 做什麼：
#   1. 檢查必要的環境變數與密碼格式
#   2. 依環境變數產生 /tmp/server.properties（broker 設定）
#   3. format 資料目錄，並在 broker 啟動「之前」寫入 admin 的 SCRAM 帳號
#   4. 產生 /tmp/admin.properties（admin 的 client 設定，給 healthcheck 與除錯用）
#   5. exec 啟動 Kafka，讓它成為容器的 PID 1
#
# Listener 設計（一個 listener 就是一扇門：port + 驗證方式）：
#   CONTROLLER  localhost:9093  SASL/PLAIN          只綁 localhost，給 broker → controller 自己用
#   INTERNAL    kafka:9092      SASL/SCRAM-SHA-512  compose 網路內的 client + inter-broker
#   EXTERNAL    localhost:9094  SASL/SCRAM-SHA-512  從 host 除錯用（compose 只 publish 到 127.0.0.1）
#
# 為什麼 controller 用 PLAIN：KRaft 的 controller listener 不支援 SCRAM
# （SCRAM 憑證本身就存在 controller 管的 metadata 裡，雞生蛋問題）。
# PLAIN 會在網路上傳明文密碼，但這扇門只綁 localhost，封包不離開本機，可以接受。
#
# 為什麼 admin 要在 format 時用 --add-scram 先寫入：inter-broker 走 SCRAM，
# broker 啟動時就要用 admin 驗證，等 broker 起來再建就來不及了。
#
# 
# =============================================================================

# -e：任何指令失敗就中止，不帶著錯誤繼續跑
# -u：用到沒定義的變數就中止（打錯變數名稱會立刻被發現，而不是默默變成空字串）
# -o pipefail：pipe 中任何一段失敗，整條 pipe 就算失敗（預設只看最後一段）
set -euo pipefail

# ---------------------------------------------------------------------------
# 1. 必填環境變數
# ---------------------------------------------------------------------------
# 「:」是什麼都不做的指令，重點在參數展開 ${X:?訊息}：
#   X 沒設定或是空字串 → 印出「訊息」到 stderr 並以非 0 結束（搭配 set -e 讓腳本中止）。
# 這是一行式的「必填檢查」，錯誤訊息直接告訴你缺了哪個變數。
# 兩個值都來自 .env，經 docker-compose.yml 的 environment 傳進容器。
: "${KAFKA_CLUSTER_ID:?KAFKA_CLUSTER_ID 未設定}"
: "${KAFKA_ADMIN_PASSWORD:?KAFKA_ADMIN_PASSWORD 未設定}"

# ---------------------------------------------------------------------------
# 2. 密碼格式檢查
# ---------------------------------------------------------------------------
# 密碼會被嵌進 JAAS 與 SCRAM 設定字串，限制成英數字以免引號 / 逗號 / 中括號破壞語法
# 例如密碼含 " 會提早結束 password="..."；含 , 或 ] 會破壞 --add-scram 的 [name=...,password=...]，
# 嚴重時等於讓密碼「注入」額外的設定。12 碼以上是基本強度要求。
# [[ 字串 =~ 正規表示式 ]] 是 bash 內建的 regex 比對；前面的 ! 表示「不符合時」。
if [[ ! "$KAFKA_ADMIN_PASSWORD" =~ ^[A-Za-z0-9]{12,}$ ]]; then
  echo "KAFKA_ADMIN_PASSWORD 需為 12 碼以上英數字" >&2
  exit 1
fi

# 之後建立的檔案權限一律是 600（只有擁有者能讀寫），因為設定檔裡有 admin 密碼。
# umask 是「要拿掉的權限」：077 = 拿掉 group 與 others 的全部權限。
umask 077

# 設定檔放 /tmp 的理由：
#   - 它不是設定來源，來源是本腳本 + 環境變數，每次啟動都重新產生
#   - 容器不是用 root 跑，/tmp 一定可寫
#   - 不放進 volume（/var/lib/kafka/data 會永久保存，含密碼的檔案不該留在那裡）
#   - 不 bind mount 到 host，host 上不留明文密碼
CONFIG=/tmp/server.properties

# ---------------------------------------------------------------------------
# 3. 產生 server.properties
# ---------------------------------------------------------------------------
# cat > 檔案 <<EOF ... EOF 是 heredoc：把中間的內容寫成檔案。
# EOF 沒有加引號，所以內容中的 ${變數} 會被展開成實際值（密碼要靠這個嵌進去）。
# 注意：也因此 heredoc 內的註解不能出現錢字號、反引號、反斜線，否則會被 shell 解讀。
# 以 # 開頭的行在 .properties 檔中是註解，Kafka 會忽略。
cat > "$CONFIG" <<EOF
# ----- KRaft 單節點 -----
# 一個 process 同時當 broker（收存訊息）與 controller（管 metadata：topic、partition、帳號、ACL）
process.roles=broker,controller
# 節點編號，要跟下一行 voters 裡的 1@... 對得上
node.id=1
# controller 投票成員只有自己：格式是「node.id@主機:port」，指向 CONTROLLER listener
controller.quorum.voters=1@localhost:9093
# 訊息與 metadata 存放位置，compose 掛了 volume kafka-data，down -v 才會清掉
log.dirs=/var/lib/kafka/data

# ----- Listener -----
# listeners：實際在哪裡監聽。0.0.0.0 = 容器內所有網卡；CONTROLLER 只綁 localhost，外部連不到
listeners=CONTROLLER://localhost:9093,INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094
# advertised.listeners：告訴 client 之後要連哪裡（兩段式連線的第二段）
#   compose 內的 client 從 INTERNAL 進來，拿到 kafka:9092（compose DNS 認得 kafka）
#   host 上的 client 從 EXTERNAL 進來，拿到 localhost:9094
#   CONTROLLER 不對 client 公開，所以不列在這裡
advertised.listeners=INTERNAL://kafka:9092,EXTERNAL://localhost:9094
# 每扇門的傳輸協定：SASL_PLAINTEXT = 要 SASL 認證，但傳輸不加密（lab 沒有 TLS）
listener.security.protocol.map=CONTROLLER:SASL_PLAINTEXT,INTERNAL:SASL_PLAINTEXT,EXTERNAL:SASL_PLAINTEXT
# 哪扇門給 controller 用
controller.listener.names=CONTROLLER
# broker 之間的通訊走哪扇門（單節點也要指定）
inter.broker.listener.name=INTERNAL

# CONTROLLER：PLAIN，帳號寫死在 JAAS（user_admin 是 server 端帳號表，username/password 是 client 端）
# 同一個 process 既是連 controller 的 client，也是被連的 server，所以兩邊都要寫：
#   username / password  → broker 以 client 身分連 controller 時用的帳密
#   user_admin=...       → controller 端的帳號表：使用者 admin、密碼是這個
# JAAS（Java Authentication and Authorization Service）是 JVM 的驗證設定格式，結尾要有分號
sasl.mechanism.controller.protocol=PLAIN
listener.name.controller.sasl.enabled.mechanisms=PLAIN
listener.name.controller.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}" user_admin="${KAFKA_ADMIN_PASSWORD}";

# INTERNAL / EXTERNAL：SCRAM，帳號存在 KRaft metadata，可執行期動態新增
# SCRAM 是 challenge-response，密碼本身不在網路上傳；broker 端只存加鹽雜湊。
# 這裡不列帳號表：producer / consumer 由 init.sh 建、面板帳號由 web 後端建，都存在 metadata。
# inter-broker 也走 SCRAM，所以 broker 自己要帶 admin 帳密登入（下面 internal 那行）
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
listener.name.internal.sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.internal.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}";
listener.name.external.sasl.enabled.mechanisms=SCRAM-SHA-512
# EXTERNAL 只接受 client、不往外連，但每個 SASL listener 仍需要一筆 server 端 JAAS entry
# 所以寫一個不帶帳密的空 entry
listener.name.external.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;

# ACL：預設拒絕，只有 admin 是 super user
# StandardAuthorizer 是 KRaft 模式的 ACL 實作，設了才會檢查權限
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
# 沒有任何 ACL 的資源 = 拒絕。這是最小權限的根基；設成 true 的話，沒設 ACL 的 topic 人人可讀寫
allow.everyone.if.no.acl.found=false
# super user 不受 ACL 限制，只給 admin（init.sh 與 web 後端用它建帳號、設 ACL）
super.users=User:admin

# 單節點：內部 topic 只能 1 份副本
# 預設是 3 份，但只有一個 broker 放不下 3 份，內部 topic 會建立失敗
# __consumer_offsets：存 consumer group 的 committed offset
offsets.topic.replication.factor=1
# __transaction_state：交易用的內部 topic，同理只能 1 份
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
# 第一個 consumer 加入 group 時不等待（預設等 3 秒看還有沒有人要加入），lab 啟動比較快
# 副作用：先加入的 consumer 會暫時拿到全部 partition，第二個加入時才 rebalance
group.initial.rebalance.delay.ms=0
# topic 一律由 kafka-init 明確建立，避免打錯字默默生出新 topic
# 例如 producer 誤寫成 oders，會直接報錯，而不是自動建一個沒人在讀的 topic
auto.create.topics.enable=false
EOF

# ---------------------------------------------------------------------------
# 4. format 資料目錄 + 寫入 admin 的 SCRAM 帳號
# ---------------------------------------------------------------------------
# KRaft 模式的資料目錄第一次使用前要先 format：寫入 cluster ID 與初始 metadata。
#   --cluster-id   ：來自 .env；同一個 cluster 的節點要一致
#   --add-scram    ：在 broker 啟動「之前」把 admin 的 SCRAM 帳號寫進初始 metadata
#                    （inter-broker 走 SCRAM，broker 一啟動就要用 admin 登入，雞生蛋問題）
# --ignore-formatted：volume 已有資料時跳過（之後改 admin 密碼不會生效，要 down -v 重來）
#                    好處是容器 restart 不會清掉資料；壞處是 SCRAM 憑證只在第一次 format 時寫入。
/opt/kafka/bin/kafka-storage.sh format \
  --config "$CONFIG" \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --add-scram "SCRAM-SHA-512=[name=admin,password=${KAFKA_ADMIN_PASSWORD}]" \
  --ignore-formatted

# ---------------------------------------------------------------------------
# 5. admin 的 client 設定檔
# ---------------------------------------------------------------------------
# admin client 設定，給 healthcheck 與 docker compose exec 除錯用（只存在容器內）
#   - docker-compose.yml 的 healthcheck：kafka-broker-api-versions.sh --command-config /tmp/admin.properties
#     用 admin 走 SCRAM 真的連一次，確認 SASL 與 ACL 都正常，不只是 port 有開
#   - 除錯：docker compose exec kafka ... --command-config /tmp/admin.properties
# 三行分別是：要 SASL 認證不加密、用 SCRAM-SHA-512、以 admin 登入。
# 受上面 umask 077 影響，權限也是 600。
cat > /tmp/admin.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}";
EOF

# ---------------------------------------------------------------------------
# 6. 啟動 Kafka
# ---------------------------------------------------------------------------
# exec：用 Kafka 取代目前的 bash process，讓 Kafka 成為容器的 PID 1。
#   docker compose stop 送出的 SIGTERM 會直接送到 Kafka，讓它正常關閉（flush、釋放 partition）。
#   沒有 exec 的話，訊號送到 bash，Kafka 收不到，逾時後被 SIGKILL 強制殺掉。
#   容器顯示 Exited (143) = 128 + 15（SIGTERM），代表是被正常停止的。
exec /opt/kafka/bin/kafka-server-start.sh "$CONFIG"
