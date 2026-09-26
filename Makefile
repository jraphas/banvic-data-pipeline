.PHONY: help setup cluster-up infra-up load-secrets deploy-postgres create-airflow-metadata-db \
        load-airflow-image build-meltano-image airflow-install sync-dags wait-dag-ready wait-webserver-ready \
        load-source-data port-forward teardown status clean

CLUSTER_NAME := banvic-cluster
NAMESPACE := banvic
MELTANO_IMAGE := banvic-meltano:latest
AIRFLOW_IMAGE := apache/airflow:2.10.5
BUSYBOX_IMAGE := busybox:stable
MELTANO_BASE_IMAGE := $(shell awk '/^FROM /{print $$2; exit}' ingestion/meltano/Dockerfile)

help: ## Lista os comandos disponiveis
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-28s\033[0m %s\n", $$1, $$2}'

setup: cluster-up infra-up load-secrets deploy-postgres create-airflow-metadata-db load-airflow-image build-meltano-image airflow-install sync-dags load-source-data wait-dag-ready wait-webserver-ready ## Inicializa e configura o ambiente para uso
	@echo ""
	@echo "Ambiente pronto (DAG ja carregada). Rode 'make port-forward' para acessar a UI do Airflow."

cluster-up: ## Cria o cluster Kind local
	@kind get clusters 2>/dev/null | grep -qx $(CLUSTER_NAME) \
		&& echo "Cluster $(CLUSTER_NAME) ja existe, pulando a criacao." \
		|| kind create cluster --name $(CLUSTER_NAME) --config infra/kind-config.yaml

infra-up: ## Cria o namespace do projeto
	kubectl apply -f infra/k8s/00-namespace.yaml

load-secrets: ## Aplica os Secrets reais
	@test -f infra/k8s/postgres/secret.yaml || \
		(echo "ERRO: copie infra/k8s/postgres/secret.example.yaml para secret.yaml e preencha antes de continuar" && exit 1)
	@test -f infra/helm/airflow-secrets.yaml || \
		(echo "ERRO: copie infra/helm/airflow-secrets.example.yaml para airflow-secrets.yaml e preencha antes de continuar" && exit 1)
	kubectl apply -f infra/k8s/postgres/secret.yaml
	kubectl apply -f infra/helm/airflow-secrets.yaml

deploy-postgres: ## Aplica o Deployment/PVC/Service do Postgres
	kubectl apply -f infra/k8s/postgres/pvc.yaml
	kubectl apply -f infra/k8s/postgres/deployment.yaml
	kubectl apply -f infra/k8s/postgres/service.yaml
	kubectl -n $(NAMESPACE) wait --for=condition=available --timeout=180s deployment/postgres

create-airflow-metadata-db: ## Cria o banco "airflow_metadata" no Postgres (metastore do Airflow)
	@test -f .env || \
		(echo "ERRO: crie o .env a partir do .env.example antes de continuar" && exit 1)
	@PG_USER=$$(grep '^POSTGRES_USER=' .env | cut -d'=' -f2); \
	if [ -z "$$PG_USER" ]; then \
		echo "ERRO: POSTGRES_USER nao esta definido no .env"; \
		exit 1; \
	fi; \
	if kubectl -n $(NAMESPACE) exec deploy/postgres -- \
	     psql -U "$$PG_USER" -tAc "SELECT 1 FROM pg_database WHERE datname='airflow_metadata'" | grep -q 1; then \
		echo "Banco airflow_metadata ja existe, pulando a criacao."; \
	else \
		echo "Criando o banco airflow_metadata..."; \
		kubectl -n $(NAMESPACE) exec deploy/postgres -- \
		  psql -U "$$PG_USER" -c "CREATE DATABASE airflow_metadata;"; \
	fi

load-airflow-image: ## Pre-carrega a imagem do Airflow no Kind
	@echo "Pre-carregando $(AIRFLOW_IMAGE) no nó do Kind"
	@docker pull $(AIRFLOW_IMAGE) >/dev/null 2>&1 || true
	@docker save --platform linux/amd64 $(AIRFLOW_IMAGE) | kind load image-archive /dev/stdin --name $(CLUSTER_NAME) >/dev/null 2>&1 || true
	@docker exec $(CLUSTER_NAME)-control-plane crictl images 2>/dev/null | grep -q 'apache/airflow' \
		&& echo "OK: imagem do Airflow disponivel no nó do Kind." \
		|| echo "AVISO: pre-carregamento nao funcionou."

build-meltano-image: ## Builda a imagem propria do Meltano e carrega no Kind
	@echo "Garantindo a imagem base $(MELTANO_BASE_IMAGE) no cache local..."
	@docker pull $(MELTANO_BASE_IMAGE) >/dev/null 2>&1 || true
	docker build -t $(MELTANO_IMAGE) ingestion/meltano/
	kind load docker-image $(MELTANO_IMAGE) --name $(CLUSTER_NAME)

airflow-install: ## Instala o Airflow via Helm chart oficial
	helm repo add apache-airflow https://airflow.apache.org
	helm repo update
	helm upgrade --install airflow apache-airflow/airflow \
		--version 1.16.0 \
		--namespace $(NAMESPACE) \
		--values infra/helm/airflow-values.yaml \
		--timeout 10m

wait-dag-ready: ## Espera a DAG aparecer no metastore
	@echo "Aguardando o DAG Processor registrar banvic_ingestion no metastore..."
	@SCHED=$$(kubectl -n $(NAMESPACE) get pod -l component=scheduler -o jsonpath='{.items[0].metadata.name}'); \
	for i in $$(seq 1 30); do \
		if kubectl -n $(NAMESPACE) exec $$SCHED -c scheduler -- airflow dags details banvic_ingestion >/dev/null 2>&1; then \
			echo "DAG banvic_ingestion registrada e visivel na UI (tentativa $$i)."; \
			exit 0; \
		fi; \
		sleep 10; \
	done; \
	echo "ERRO: a DAG nao foi registrada em 5 minutos. Investigue com:"; \
	echo "  kubectl -n $(NAMESPACE) exec deploy/airflow-scheduler -c scheduler -- airflow dags list-import-errors"; \
	echo "  kubectl -n $(NAMESPACE) exec deploy/airflow-scheduler -c scheduler -- tail -30 /opt/airflow/logs/dag_processor_manager/dag_processor_manager.log"; \
	exit 1

wait-webserver-ready: ## Espera a UI do Airflow estar de fato respondendo
	@echo "Aguardando o webserver do Airflow ficar pronto..."
	@kubectl -n $(NAMESPACE) wait --for=condition=ready --timeout=600s pod -l component=webserver \
		&& echo "UI do Airflow pronta." \
		|| (echo "ERRO: o webserver nao ficou pronto em 10 minutos. Investigue com:"; \
		    echo "  kubectl -n $(NAMESPACE) logs -l component=webserver -c webserver --previous --tail=30"; \
		    exit 1)

sync-dags: ## Copia dags/ local para dentro do PVC usado pelo Airflow
	kubectl -n $(NAMESPACE) wait --for=condition=ready --timeout=180s pod -l component=scheduler
	kubectl -n $(NAMESPACE) cp dags/. $$(kubectl -n $(NAMESPACE) get pod -l component=scheduler -o jsonpath='{.items[0].metadata.name}'):/opt/airflow/dags -c scheduler
	@echo "DAGs copiadas. Rode este comando de novo sempre que editar algo em dags/."

load-source-data: ## Copia o banvic_data.zip extraido para dentro do cluster (PVC de origem)
	@test -d data/csv || (echo "ERRO: extraia o banvic_data.zip para data/csv/ antes de rodar isso" && exit 1)
	kubectl apply -f infra/k8s/source-data-pvc.yaml
	kubectl -n $(NAMESPACE) delete pod data-loader --ignore-not-found
	kubectl -n $(NAMESPACE) run data-loader --image=$(BUSYBOX_IMAGE) --restart=Never \
		--overrides='{"spec":{"containers":[{"name":"data-loader","image":"$(BUSYBOX_IMAGE)","command":["sleep","3600"],"volumeMounts":[{"name":"data","mountPath":"/data"}]}],"volumes":[{"name":"data","persistentVolumeClaim":{"claimName":"banvic-source-data-pvc"}}]}}'
	kubectl -n $(NAMESPACE) wait --for=condition=ready pod/data-loader --timeout=60s
	kubectl -n $(NAMESPACE) cp data/csv/. data-loader:/data
	kubectl -n $(NAMESPACE) delete pod data-loader

port-forward: wait-webserver-ready ## Expoe a UI do Airflow em localhost:8080
	kubectl -n $(NAMESPACE) port-forward svc/airflow-webserver 8080:8080

status: ## Mostra o status de todos os pods do projeto
	kubectl -n $(NAMESPACE) get pods,pvc,svc

teardown: ## Remove todos os recursos do namespace
	kubectl delete namespace $(NAMESPACE)

clean: ## Destroi o cluster Kind inteiro
	kind delete cluster --name $(CLUSTER_NAME)
