FROM node:lts-alpine AS build

WORKDIR /build

COPY package.json package-lock.json ./
RUN npm ci

COPY setup-ui/ ./setup-ui/
RUN npm run build:setup-ui

FROM node:lts-alpine AS run

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY src/ ./src/
COPY bin/ ./bin/
COPY docs/ ./docs/
COPY --from=build /build/dist/ ./dist/

COPY bin/docker-entrypoint.mjs ./bin/docker-entrypoint.mjs

RUN apk add --no-cache curl

RUN addgroup -S replicore && adduser -S replicore -G replicore
RUN mkdir -p /data && chown replicore:replicore /data

EXPOSE 3000
EXPOSE 49737/udp

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -sf http://127.0.0.1:${REPLICORE_HTTP_PORT:-3000}/status/leader || exit 1

USER replicore

ENTRYPOINT ["node", "bin/docker-entrypoint.mjs"]
