.PHONY: all build build-arm64 build-amd64 test test-chaos bench fmt lint docker-build docker-push k3s-deploy k3s-teardown clean

REGISTRY ?= localhost:5000
TAG ?= latest

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

k3s-deploy:
	kubectl apply -f deploy/k8s/rbac.yaml
	kubectl apply -f deploy/k8s/configmap.yaml
	kubectl apply -f deploy/k8s/master-service.yaml
	kubectl apply -f deploy/k8s/master-pdb.yaml
	kubectl apply -f deploy/k8s/master-deployment.yaml
	kubectl apply -f deploy/k8s/chunkserver-daemonset.yaml

k3s-teardown:
	kubectl delete -f deploy/k8s/chunkserver-daemonset.yaml --ignore-not-found
	kubectl delete -f deploy/k8s/master-deployment.yaml --ignore-not-found
	kubectl delete -f deploy/k8s/master-pdb.yaml --ignore-not-found
	kubectl delete -f deploy/k8s/master-service.yaml --ignore-not-found
	kubectl delete -f deploy/k8s/configmap.yaml --ignore-not-found
	kubectl delete -f deploy/k8s/rbac.yaml --ignore-not-found

clean:
	cargo clean
