# Repository Structure

Repository layout aligned to the finalized SRS (`AI_Event_Processing_SRS_V2.pdf`, Version 1.0) — see [`docs/PLAN.md`](PLAN.md) for phase status. Area owners are listed in [`CONTRACTS.md`](CONTRACTS.md) §9.

```
ai-driven-system/
├── README.md                          Root documentation
├── LICENSE                            MIT
├── Makefile                           Build / test / up / down shortcuts               
├── Jenkinsfile                        CI/CD: build, test, promtool, terraform, images, optional load test, push
├── .gitignore
│
├── services/                          All deployable backend microservices
│   ├── ingestion-service/             Java 17 / Spring Boot 3 — data path (REQ-001 to REQ-005, REQ-008)
│   │   ├── pom.xml, mvnw, Dockerfile
│   │   └── src/
│   │       ├── main/java/com/eventproc/ingestion/
│   │       ├── main/resources/application.yml
│   │       └── test/java/...          Unit + Kafka integration tests
│   │
│   ├── forecasting-service/           Python 3.11 / FastAPI — predictive analytics (REQ-021 to REQ-025)
│   │   ├── requirements.txt, requirements-ml.txt, requirements-dev.txt, Dockerfile
│   │   ├── config/components.yaml     Components, PromQL, thresholds (hot-reloaded)
│   │   ├── training/                  train_forecast_model.py, train_tft.py
│   │   ├── models/                    Trained model JSON/checkpoints (git-ignored)
│   │   ├── app/                       FastAPI app, forecasting engine, predictive analytics, alert store
│   │   └── tests/                     Unit & API tests
│   │
│   ├── diagnostic-service/            Python 3.11 / FastAPI — anomaly detection + GraphRAG root cause (ADR-0010)
│   │   ├── requirements.txt, requirements-dev.txt, Dockerfile
│   │   ├── app/                       FastAPI app: /score, /thresholds, GraphRAG diagnostic engine, LLM client
│   │   ├── data/dependency_graph.json Service catalogue: 15 components and their edges
│   │   └── tests/                     Scorer, thresholds, audit, consumer, API, graph, LLM tests
│   │
│   ├── event-archiver/                Kafka → S3 archiver (REQ-028 to REQ-030, ADR-0009)
│   │   ├── requirements.txt, requirements-dev.txt, Dockerfile
│   │   ├── archiver.py                Consumer: batch per partition, upload, commit after upload
│   │   ├── archive_client/client.py   ArchiveClient: archive_batch / archive (boto3; MinIO or S3)
│   │   └── tests/                     Fake S3 + consumer tests
│   │
│   └── web-console/                   Demo dashboard on :9099 (FastAPI backend + static HTML/CSS/JS)
│       ├── app.py, requirements.txt, Dockerfile
│       └── static/                    index.html, console.js, console.css
│
├── contracts/                         Machine-readable contracts (JSON Schema 2020-12)
│   ├── README.md
│   ├── raw-event.schema.json          Kafka raw-events value
│   ├── normalized-event.schema.json   Kafka normalized-events value
│   └── predictive-alert.schema.json   PredictiveAlert
│
├── docs/
│   ├── CONTRACTS.md                   Binding integration spec (source of truth)
│   ├── ARCHITECTURE.md                C4 views, runtime sequences, failure modes, scaling
│   ├── DASHBOARDS.md                  REQ-034 Grafana dashboards guide
│   ├── adr/                           Architecture Decision Records (0001-0010)
│   ├── SLO.md                         Service-level objectives                         
│   ├── runbooks/                      Operational runbooks per alert
│   ├── jira/                          Jira backlog import (jira-import.csv)
│   ├── PLAN.md                        Implementation road-map
│   ├── STRUCTURE.md                   (this file)
│   └── sdlc-justification.md          SDLC methodology rationale
│
├── infra/
│   ├── docker-compose.yml             Kafka, ZK, Prometheus, Grafana, MinIO, all services
│   ├── docker-compose.aws.yml         Override: archive to your real S3 bucket
│   ├── prometheus.yml                 Scrape config (adds `service` label per job)
│   ├── prometheus/rules/*.yml         Recording + alert rules → /etc/prometheus/rules
│   ├── grafana/
│   │   ├── provisioning/              Data source (Prometheus), dashboard providers
│   │   └── dashboards/                Dashboard JSON                                   
│   ├── k8s/
│   │   ├── ingestion-deployment.yaml  Namespace, ConfigMap, Deployment, Service
│   │   ├── diagnostic-deployment.yaml ConfigMap, Deployment (2-6 pods, Kafka consumer), Service
│   │   ├── archiver-deployment.yaml   IRSA ServiceAccount, ConfigMap, Deployment, Service
│   │   ├── ingress.yaml               ALB Ingress, TLS 1.2+ (REQ-047)
│   │   └── hpa.yaml                   HorizontalPodAutoscalers (ingestion, diagnostic)
│   └── terraform/                     AWS (REQ-044); azurerm block commented out
│       ├── main.tf, variables.tf, outputs.tf, terraform.tfvars.example
│       ├── eks.tf                     EKS cluster + node group
│       ├── s3.tf                      Archive bucket (lifecycle, SSE, TLS-only) + archiver IRSA role
│       └── ecr.tf                     Image repositories
│
├── tests/
│   ├── load/                          JMeter load test (REQ-001/004): jmeter/*.jmx, run-load-test.sh, analyze.py
│   └── reports/                       Test execution reports (Postman HTML report, etc.)
│
├── ci/jenkins/                        Local Jenkins in Docker that runs the Jenkinsfile (run-local.sh)
│
└── scripts/
    ├── setup-local.sh                 Bootstrap the local stack
    ├── produce-events.sh              Send valid / invalid sample events to raw-events
    └── demo.sh                        Guided CLI demo script
``` 
```

## Naming Conventions

| Layer | Convention |
|---|---|
| Java packages | `com.eventproc.<service>.<layer>` |
| Python modules | `snake_case` |
| JSON fields (all contracts) | `snake_case` |
| Component ids | lowercase kebab-case, e.g. `forecasting-service` |
| IDs | `al-<12 hex>` (alerts) |
| Prometheus metrics | Fixed names in CONTRACTS.md §7 |
| ADRs | `docs/adr/NNNN-kebab-title.md`, never renumbered |
| Kubernetes labels | `app.kubernetes.io/name`, `app.kubernetes.io/component` |
| Docker image tags | `ghcr.io/<org>/<service>:<tag>` |

## Port Assignments

| Service | Host port (compose) | Service port (in-network, k8s) |
|---|---|---|
| ingestion-service | 9080 | 8080 |
| forecasting-service | 9081 | 8081 |
| diagnostic-service | 9082 | 8082 |
| event-archiver metrics | 9083 | 8083 |
| Grafana | 9030 | 3000 |
| Prometheus | 9090 | 9090 |
| Kafka broker | 9092 | `kafka:29092` |
| Zookeeper | — | 2181 |
| MinIO (REQ-028 local S3 stand-in) | 9000 (API) / 9001 (console) | same |
| Jenkins (local CI) | 9095 | 8080 |
