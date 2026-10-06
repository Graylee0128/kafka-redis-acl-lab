# 以官方 Kafka 映像為基底：後端要呼叫 kafka-configs.sh 建立 SCRAM 帳號
# （Node 的 Kafka client 都沒有實作 AlterUserScramCredentials）
FROM apache/kafka:3.9.1

USER root
RUN apk add --no-cache nodejs npm

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force && apk del npm

COPY src ./src
COPY public ./public

ENV NODE_ENV=production KAFKA_BIN=/opt/kafka/bin PORT=8080
USER appuser
EXPOSE 8080

# 覆寫基底映像的 Kafka 啟動腳本
ENTRYPOINT []
CMD ["node", "src/server.js"]
