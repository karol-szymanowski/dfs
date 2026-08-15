.PHONY: all build build-arm64 build-amd64 test test-chaos test-cluster bench fmt lint docker-build docker-push helm-lint helm-template helm-install helm-upgrade helm-uninstall clean

REGISTRY ?= localhost:5000
TAG ?= latest
NAMESPACE ?= default
RELEASE_NAME ?= gfs

all: lint test build

build:
	cargo build --workspace

build-arm64:
	cargo build --workspace --target aarch64-unknown-linux-musl --release

build-amd64:
	cargo build --workspace --target x86_64-unknown-linux-musl --release

test:
	cargo test --workspace --all-targets

test-chaos:
	cargo test -p gfs-chaos -- --nocapture

test-cluster: build
	./scripts/test-local-cluster.sh

bench:
	cargo run --release -p gfs-bench

fmt:
	cargo fmt --all

lint:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings

docker-build:
	docker buildx build --platform linux/arm64,linux/amd64 -t $(REGISTRY)/gfs-master:$(TAG) -f deploy/docker/Dockerfile.master .
	docker buildx build --platform linux/arm64,linux/amd64 -t $(REGISTRY)/gfs-chunkserver:$(TAG) -f deploy/docker/Dockerfile.chunkserver .
	docker buildx build --platform linux/arm64,linux/amd64 -t $(REGISTRY)/gfs-fuse:$(TAG) -f deploy/docker/Dockerfile.fuse .

docker-push:
	docker push $(REGISTRY)/gfs-master:$(TAG)
	docker push $(REGISTRY)/gfs-chunkserver:$(TAG)
	docker push $(REGISTRY)/gfs-fuse:$(TAG)

helm-lint:
	helm lint deploy/helm/gfs

helm-template:
	helm template $(RELEASE_NAME) deploy/helm/gfs -n $(NAMESPACE)

helm-install:
	helm install $(RELEASE_NAME) deploy/helm/gfs -n $(NAMESPACE) --create-namespace

helm-upgrade:
	helm upgrade --install $(RELEASE_NAME) deploy/helm/gfs -n $(NAMESPACE)

helm-uninstall:
	helm uninstall $(RELEASE_NAME) -n $(NAMESPACE)

clean:
	cargo clean
