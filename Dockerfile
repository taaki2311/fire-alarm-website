FROM rust:1.96-trixie AS builder
WORKDIR /home/fire-alarm-website
COPY src/ src/
COPY Cargo.lock .
COPY Cargo.toml .
RUN cargo build --release

FROM debian:trixie-slim
WORKDIR /home
ARG DEBIAN_FRONTEND=noninteractive
RUN apt update && apt full-upgrade --yes && apt install curl --yes && \
    curl --show-error --silent https://dotenvx.sh/install.sh | sh && \
    apt remove curl --yes && apt autoremove --yes && apt clean
COPY .env.test .
COPY email.html .
COPY index.html .
COPY index.js .
COPY style.css .
COPY --from=builder /home/fire-alarm-website/target/release/fire-alarm-website /usr/local/bin/

ENTRYPOINT [ "dotenvx", "run", "--" ]
CMD [ "fire-alarm-website" ]