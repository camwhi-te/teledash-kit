# Build: compile the service into one self-contained binary (migrations and schemas embedded)
FROM oven/bun:1.4.4 AS build
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production
COPY . .
RUN bun build ./src/main.ts --compile --minify --sourcemap --outfile /out/service

# Run: glibc-only distroless base, no shell, non-root
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /out/service /service
ENV HTTP_PORT=8080
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=5s --retries=3 CMD ["/service", "--healthcheck"]
ENTRYPOINT ["/service"]
