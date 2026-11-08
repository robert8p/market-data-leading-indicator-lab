FROM node:22-bookworm-slim
ENV NODE_ENV=production
WORKDIR /app
RUN command -v flock && command -v mountpoint
COPY worker.cjs evidence.cjs start.sh ./
CMD ["/bin/sh","/app/start.sh"]
