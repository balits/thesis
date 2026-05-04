KAVE = kave
APP_NAME = $(KAVE)
NAMESPACE = $(APP_NAME)
CHART = ./charts/kave
KUBECONFIG ?= civo-kave-kubeconfig # overriden on github actions

DOCKERFILE = docker/Dockerfile
COMPOSE3 = docker-compose3.yaml

REGISTRY = ghcr.io
REPO = balits/kave
IMG_NAME_NO_REGISTRY=$(REPO)
IMG_NAME = $(REGISTRY)/$(IMG_NAME_NO_REGISTRY)
IMAGE_TAG ?= ""

.DEFAULT_GOAL := help

build-go: ## Build Go binary
	@echo "> building ${APP_NAME}..."
	go build -o bin/${APP_NAME} cmd/kave/main.go

build-img: ## Build Docker image
	@echo "> builing img: ${IMG_NAME}..."
	$(MAKE) build
	docker build -t ${IMG_NAME}:latest .

build-img-push:
	$(MAKE) build-img
	@echo "> loggin in to ghcr.io..."
	gh auth token | docker login ghcr.io -u balits --password-stdin
	@echo "> pushing img to ghcr: ${IMG_NAME}..."
	docker push ${IMG_NAME}:latest


up1build: ## Builds image and runs 1-node cluster
	$(MAKE) build-img
	$(MAKE) up1

up1: ## Run 1-node cluster
	docker compose -f $(COMPOSE1) -p $(APP_NAME) up --remove-orphans

up3: ## Run 3-node cluster
	docker compose -f $(COMPOSE3) -p $(APP_NAME) up --remove-orphans

down3:
	docker compose -f $(COMPOSE3) -p $(APP_NAME) down --volumes

up3build: ## Builds image and runs 3-node cluster
	$(MAKE) build-img
	$(MAKE) up3

test-unit:
	go test -v ./internal/... --timeout=90s

test-integ-stress:
	@echo "> running integrations stress (with stress tests)"
	go test ./test/integration/... --timeout 10m -p 1 --tags=stress
test-integ:
	@echo "> running integrations stress (without stress tests)"
	go test ./test/integration/... --timeout 10m -p 1

test-smoke:
	@echo "> 1.1 init: deleting previous Kind cluster..."
	kind delete cluster --name $(APP_NAME) || true

	@echo "> 1.2 init: creating local Kind cluster..."
	kind create cluster --name $(APP_NAME)

	@echo "> 1.3 init: load image to Kind"
	kind load docker-image $(IMG_NAME):latest --name $(APP_NAME)
	
	@echo "> 2. installing Helm Chart..."
	helm upgrade --install $(APP_NAME) ./charts/kave \
		--namespace $(APP_NAME) \
		--create-namespace \
		--set image.pullPolicy=Never \
		--set image.registry=$(REGISTRY) \
		--set image.repository=$(REPO) \
		--set image.tag=$(IMAGE_TAG) \
		--set config.storage.kind=boltdb \
		--set config.ratelimiter.read.rps=2000 \
		--set config.ratelimiter.read.burst=2000 \
		--set config.ratelimiter.write.rps=2000 \
		--set config.ratelimiter.write.burst=2000 \
 		--set voters.replicas=3 \
		--wait \
		--timeout=3m

	@echo "> 4 piping kind kubeconfig..."
	kind get kubeconfig --name $(APP_NAME) > /tmp/kind-kubeconfig-$(APP_NAME).yaml

	# kill any existing port-forwards on 8080
	-fuser -k 8080/tcp || true
	sleep 5

	@echo "> 5.2 Smoke Tests: running smoke tests..."
	@( \
	echo "Starting resilient port-forward in background..."; \
		(while true; do \
			kubectl port-forward svc/kave-external 8080:80 \
				--namespace $(APP_NAME) \
				--kubeconfig /tmp/kind-kubeconfig-$(APP_NAME).yaml > /dev/null 2>&1; \
			sleep 1; \
		done) & \
		LOOP_PID=$$!; \
		echo "Port-forward loop PID: $$LOOP_PID. Waiting 5 seconds..."; \
		sleep 5; \
		KUBECONFIG=/tmp/kind-kubeconfig-$(APP_NAME).yaml \
		NAMESPACE=$(APP_NAME) \
		CLUSTER_SIZE=3 \
		URL=http://localhost:8080 \
		go test ./test/smoke/... -v -count=1 --timeout 5m; \
		test_result=$$?; \
		echo "Cleaning up port-forward loop..."; \
		kill -9 $$LOOP_PID 2>/dev/null || true; \
		fuser -k 8080/tcp 2>/dev/null || true; \
		exit $$test_result)

	@echo "> 6. cleaning up port-forward..."
	-fuser -k 8080/tcp || true

test-race:
	go test -v -race -count=3 ./internal/... 

kind-cluster-create: ## Creates a k8s cluster with kind
	kind create cluster --name kave

kind-verify: ## Verifyies kind cluster
	kubectl get nodes

helm-validate: ## validate template
	helm template $(APP_NAME) ./charts/kave

helm-lint: ## Lint for mistakes
	helm lint ./charts/kave \
		--set image.tag=lint-test \
		--set config.adminAuthToken=lint-test

helm-uninstall:
	@echo ">> uninstalling chart: ${APP_NAME}..."
	helm uninstall kave -n kave --ignore-not-found --kubeconfig $(KUBECONFIG)

helm-upgrade-install:
	@echo ">> patching Chart.yaml appVersion..."
	yq eval '.appVersion = "$(IMAGE_TAG)"' -i $(CHART)/Chart.yaml
	@echo ">> upgrading/installing chart: ${APP_NAME}..."
	helm upgrade $(APP_NAME) $(CHART) --install \
		--kubeconfig $(KUBECONFIG) \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--set traefik.enabled=true \
		--set image.tag=$(IMAGE_TAG) \
		--wait \
		--timeout 3m

helm-upgrade-install-no-traefik:
	@echo ">> upgrading/installing chart: ${APP_NAME}..."
	helm upgrade $(APP_NAME) $(CHART) --install --namespace $(NAMESPACE) \
		--kubeconfig $(KUBECONFIG) \
		--create-namespace \
		--wait \
		--timeout 3m

local-helm-recreate:
	$(MAKE) helm-uninstall
	$(MAKE) helm-upgrade-install-no-traefik

gh-ci:
	gh workflow run ci.yaml --ref feature/ci

local-kube-recreate-cluster:
	@echo ">> deleting namesapce ${NAMESPACE}..."
	kubectl delete namespace kave --kubeconfig .kube/config
	@echo ">> creating namesapce ${NAMESPACE}..."
	kubectl create namespace kave --kubeconfig .kube/config

local-kube-restart-rollout:
	@echo ">> rollout/restart kave-voter statefulset..."
	kubectl rollout restart statefulset/kave-voter -n $(NAMESPACE) --kubeconfig .kube/config

wipe-recreate-cluster: ## Wipe and redeploy helm chart
	@echo ">> 1. step: uninstall any chart"
	$(MAKE) helm-uninstall
	@echo ">> 1. step: ok"

	@echo ">> 2. step: deleting k8s persistentVolumeClaims..."
	kubectl delete pvc --all -n $(NAMESPACE) --ignore-not-found --kubeconfig $(KUBECONFIG)
	kubectl wait --for=delete pvc --all -n kave --timeout=1m || true
	@echo ">> 2. step: ok"

	@echo ">> 3. step: insalling/upgrading chart ${APP_NAME}..."
	$(MAKE) helm-upgrade-install
	@echo ">> 3. step: ok"
	@echo ">> DONE"

wait-cluster-ready:
	@echo ">> 1. waiting on rollout status"
	kubectl rollout status statefulset/kave-voter \
		--namespace $(NAMESPACE) \
		--kubeconfig $(KUBECONFIG) \
		--timeout=3m

	@echo ">> 2. printing final pod state"
	kubectl get pods \
		--namespace $(NAMESPACE) \
		--kubeconfig $(KUBECONFIG)

next:
	@echo "feature/next: update cluster..."
	yq eval '.appVersion = "$(IMAGE_TAG)"' -i $(CHART)/Chart.yaml
	helm upgrade $(APP_NAME) $(CHART) \
		--kubeconfig $(KUBECONFIG) \
		--namespace $(NAMESPACE) \
		--install \
		--reuse-values	\
		--set prometheus.enabled=true \
		--set traefik.enabled=true \
		--set image.tag=$(IMAGE_TAG) \
		-- wait \
		--timeout 10m


k9s:
	./bin/k9s --kubeconfig .kube/config 
fmt:
	./bin/golangci-lint fmt
lint:
	./bin/golangci-lint run

loc: ## prints total lines of code
	git ls-files | xargs wc -l

.PHONY: build build-img up3 down3 up3build test test-race kluster-create, kluster-verify helm-validate helm-lint helm-uninstall gh-ci
