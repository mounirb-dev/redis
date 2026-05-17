FROM oven/bun:1 AS install
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

FROM oven/bun:1 AS release
WORKDIR /app
ENV NODE_ENV=production
COPY --from=install /app/node_modules ./node_modules
COPY package.json bun.lock ./
COPY src ./src
EXPOSE 3000
CMD ["bun", "run", "src/index.ts"]
