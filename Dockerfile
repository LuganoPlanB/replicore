FROM node:lts-slim AS build

WORKDIR /build

COPY package.json package-lock.json ./
RUN npm ci

COPY setup-ui/ ./setup-ui/
RUN npm run build:setup-ui

FROM node:lts-slim AS run

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY src/ ./src/
COPY bin/ ./bin/
COPY docs/ ./docs/
COPY --from=build /build/dist/ ./dist/

COPY bin/docker-entrypoint.mjs ./bin/docker-entrypoint.mjs

RUN apt-get update && apt-get install -y --no-install-recommends curl libatomic1 gosu && rm -rf /var/lib/apt/lists/*

RUN groupadd -r replicore && useradd -r -g replicore replicore
RUN mkdir -p /data && chown replicore:replicore /data

COPY bin/docker-startup.sh /usr/local/bin/docker-startup.sh
RUN chmod +x /usr/local/bin/docker-startup.sh

EXPOSE 3000
EXPOSE 49737/udp

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -sf http://127.0.0.1:${REPLICORE_HTTP_PORT:-3000}/status/leader || exit 1

ENTRYPOINT ["/usr/local/bin/docker-startup.sh"]
