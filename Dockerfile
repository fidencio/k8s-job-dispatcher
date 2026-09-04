# Copyright (c) 2026 Kata Contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# Static via musl on amd64 and arm64. ppc64le and s390x link against glibc and
# take a runtime image providing it: s390x has no musl std published at all, and
# musl's ppc64le support is a variable nobody can test here, since no CI runner
# executes that architecture natively.

# The only place the compiler is named: CI installs this same version, and
# dependabot bumps it here. Not the MSRV, which is Cargo.toml's rust-version.
FROM rust:1.98.0-trixie AS rust-base

# ring compiles C, and the cc crate wants musl-gcc once the target is musl.
FROM rust-base AS musl-base
# hadolint ignore=DL3008
RUN apt-get update && \
	apt-get install -y --no-install-recommends musl-tools && \
	rm -rf /var/lib/apt/lists/*

FROM musl-base AS toolchain-amd64
ENV RUST_TARGET=x86_64-unknown-linux-musl

FROM musl-base AS toolchain-arm64
ENV RUST_TARGET=aarch64-unknown-linux-musl

FROM rust-base AS toolchain-ppc64le
ENV RUST_TARGET=powerpc64le-unknown-linux-gnu

FROM rust-base AS toolchain-s390x
ENV RUST_TARGET=s390x-unknown-linux-gnu

# Resolves to a stage above, which hadolint cannot see.
# hadolint ignore=DL3006
FROM toolchain-${TARGETARCH} AS builder

WORKDIR /src

RUN rustup target add "${RUST_TARGET}"

# Dependencies resolve from the manifests alone, so a placeholder entry point
# keeps them in a layer that survives source changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && \
	cargo build --release --locked --target "${RUST_TARGET}" && \
	rm -rf src

COPY src ./src

# COPY preserves the context's mtimes, so the real entry point has to be made
# newer than the placeholder cargo already built.
RUN touch src/main.rs && \
	cargo build --release --locked --target "${RUST_TARGET}" && \
	cp "target/${RUST_TARGET}/release/k8s-job-dispatcher" /usr/local/bin/k8s-job-dispatcher

# The notices are one text for every architecture, so this runs on the build
# host rather than four times under emulation. --platform is ignored on a FROM
# naming a stage, which is why this repeats the image instead of deriving from
# rust-base; dependabot moves both lines as one dependency.
FROM --platform=$BUILDPLATFORM rust:1.98.0-trixie AS notices

WORKDIR /src

# Ahead of the manifests, so a dependency bump does not rebuild the tool. The
# binary sits behind `cli`.
RUN cargo install cargo-about --version 0.9.2 --locked --features cli

# Needs an entry point to exist, but never compiles one.
COPY Cargo.toml Cargo.lock about.toml about.hbs ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && \
	cargo about generate about.hbs -o THIRD-PARTY-NOTICES.txt && \
	rm -rf src

# Lets a release ship the binary without anyone unpacking an image:
# --target binary --output type=local.
FROM scratch AS binary
COPY --from=builder /usr/local/bin/k8s-job-dispatcher /
COPY --from=notices /src/THIRD-PARTY-NOTICES.txt /
COPY LICENSE /

# distroless publishes only rolling tags, so :latest is how it is consumed.
# hadolint ignore=DL3007
FROM gcr.io/distroless/static-debian13:latest AS runtime-amd64
# hadolint ignore=DL3007
FROM gcr.io/distroless/static-debian13:latest AS runtime-arm64

# `static` plus the loader and glibc the dynamic builds need; nossl because kube
# uses rustls. It has no libgcc, where Rust's unwinder lives, and the prebuilt
# libstd records that dependency - without this copy the binary will not start.
# hadolint ignore=DL3007
FROM gcr.io/distroless/base-nossl-debian13:latest AS runtime-glibc
COPY --from=builder /usr/lib/*-linux-gnu/libgcc_s.so.1 /usr/lib/

FROM runtime-glibc AS runtime-ppc64le
FROM runtime-glibc AS runtime-s390x

# hadolint ignore=DL3006
FROM runtime-${TARGETARCH}

COPY --from=builder /usr/local/bin/k8s-job-dispatcher /usr/bin/k8s-job-dispatcher

# Where a licence scanner looks.
COPY --from=notices /src/THIRD-PARTY-NOTICES.txt /usr/share/doc/k8s-job-dispatcher/
COPY LICENSE /usr/share/doc/k8s-job-dispatcher/

USER 65532:65532

ENTRYPOINT ["/usr/bin/k8s-job-dispatcher"]
