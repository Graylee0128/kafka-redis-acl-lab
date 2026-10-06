# 🚀 開發作業：Kafka 與 Redis 整合及權限控制實作

> 本檔為題目原文。繳交說明（啟動指令、帳號設計、驗證方式）見 [README.md](README.md)。

## 📝 測驗目的

本作業旨在評估開發者對於容器化部署（Docker）、訊息佇列（Kafka）、內存快取（Redis）以及權限控制（ACL）的整合與架構設計能力，並驗證對 Kafka 訊息順序性與 Consumer Group 負載平衡的理解。

## ⚙️ 系統架構要求

你需要完成一個微服務架構，所有服務都必須透過 `docker-compose` 進行一鍵部署：

1. Redis Server（需啟用 ACL）
2. Kafka Server（需啟用 ACL）
3. Producer 應用程式（需打包為容器）
4. Consumer 應用程式（需打包為容器，並啟動 2 個實體）

## 🗺️ 架構圖

### 圖一：原題目（Core）

![原題目架構圖](docs/img/architecture-core.png)

> 原始碼：[`docs/img/architecture-core.mmd`](docs/img/architecture-core.mmd)（Mermaid）

重點對照：

- **順序性**：相同 `order_id` → 相同 hash → 同一個 Partition → 只會被一個 Consumer 依序處理。
- **負載平衡**：同一個 Consumer Group 內，每個 Partition 只分配給一個 Consumer；Partition 數（3）> Consumer 數（2），兩個實體都會分到工作。
- **最小權限**：Producer 只能寫、Consumer 只能讀 + 加入 Group；Redis 帳號只能存取 `myapp:*` 前綴的 Key。

### 圖二：加分題（權限申請面板）

![加分題架構圖](docs/img/architecture-bonus.png)

> 原始碼：[`docs/img/architecture-bonus.mmd`](docs/img/architecture-bonus.mmd)（Mermaid）

設計備註：

- 動態建立 Kafka 帳號需使用 **SASL/SCRAM**（憑證存於 KRaft metadata / ZooKeeper，可透過 Admin API 新增）；SASL/PLAIN 的帳號寫死在 JAAS 設定檔，無法在執行期動態新增。
- Web 後端是唯一持有 Kafka / Redis admin 帳號的元件，admin 憑證透過環境變數注入，不寫進程式碼。
- 新帳號預設只授予最小權限（例如 Kafka：READ 指定 Topic；Redis：`~myapp:*` + 讀取指令）。

## 📌 基本需求限制（Core Requirements）

### 1. 基礎設施（Infrastructure）

- 必須提供一份 `docker-compose.yml`，使用 `docker-compose up -d` 即可啟動所有服務。
- **Redis**：必須開啟 ACL（Access Control List）認證，禁止使用預設的無密碼公開存取。
- **Kafka**：必須開啟 ACL 認證（如 SASL/PLAIN 或其他機制），確保讀寫 Topic 需要帳號密碼驗證。
- **Kafka Topic 規劃**：為配合多個 Consumer 的驗證，該 Topic 的 Partition 數量必須大於 1（例如設定為 2 或 3）。

### 2. 開發應用程式（Application）

你可以使用任何程式語言（Node.js、Python、Go、Java 等）來開發以下兩個程式，且這兩個專案都必須撰寫 Dockerfile 包裝成容器服務。

#### Producer（生產者）

- 定期或持續產出模擬數據（格式自訂，例如 JSON 格式的模擬訂單或日誌）。
- 程式需使用帶有權限的帳號連線至 Kafka，並將訊息發送至指定的 Topic 中。
- **順序性要求**：發送訊息時，必須設定 Message Key（例如 `user_id`、`order_id` 等），以確保相同 Key 的訊息會被分配到同一個 Partition，保證特定資料的處理順序性。

#### Consumer（消費者）

- 需透過 `docker-compose` 啟動 **2 個** Consumer 實體（Instances）。
- 這兩個實體必須設定為**同一個 Consumer Group**，藉此展示 Kafka 的負載平衡機制。
- 程式需使用帶有權限的帳號連線至 Kafka 訂閱該 Topic 並接收訊息。
- 接收到訊息後，必須將資料儲存至 Redis 中。
- **前綴要求**：存入 Redis 的 Key 必須設定特定的 Prefix（前綴），例如：

  ```text
  myapp:events:<id>
  ```

## ⭐ 加分題（Bonus）

開發一個簡單的 Web 網頁服務（Web UI，同樣需包裝在 Docker Compose 內）：

- 實作一個「權限申請面板」。
- 允許使用者在網頁上輸入「自訂帳號」與「密碼」來申請權限。
- 後端接收到申請後，透過程式動態建立相對應的 Kafka ACL 或是 Redis ACL 帳號，讓該帳號獲得存取權。

## 📦 繳交與驗收方式

1. 請提供一個完整的專案資料夾（或 GitHub 連結），包含所有原始碼、Dockerfile 與 `docker-compose.yml`。
2. 專案根目錄需附上一份 `README.md`，說明：
   - 專案的啟動指令。
   - 你所設計的 Redis/Kafka ACL 帳號密碼與配置說明。
   - 如何驗證 2 個 Consumer 成功分擔工作，且相同 Key 的訊息有保持順序。
   - （如有實作加分題）網頁的本機測試 Port 與操作流程。
3. 確保專案在乾淨的環境下，執行：

   ```bash
   docker-compose up --build
   ```

   後能順利無誤地自動運行。
