FROM node:22-alpine

ENV NODE_ENV=production
WORKDIR /app

# 先只複製依賴清單，原始碼改動時可沿用 npm ci 這層快取
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY src ./src

USER node
CMD ["node", "src/index.js"]
