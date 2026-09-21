# Eyego Activity Stream

This repository contains a practical MVP for an event-driven user activity processing pipeline using Node.js, Kafka, MongoDB, and Express.

## 1. DDD and architecture

### Bounded contexts

- User Activity Capture: receives user events from web/mobile apps and emits domain events.
- Activity Processing: consumers validate, enrich, and persist processed log entries.
- Activity Query API: exposes read-models for dashboards and audits.

### Domain objects

- UserActivity: the emitted event from the client side, representing a single user action.
- ActivityProcessed: the persisted processed log entry used by query and analytics services.

### Layered structure

- domain: immutable value objects, event types, aggregates, validation rules.
- application: use cases such as `ActivityLogService` and `ActivityQueryService`.
- infrastructure: Kafka producer/consumer, MongoDB repository, env config, logging.

> This split keeps the domain logic independent from transport concerns and makes future changes to Kafka or MongoDB safer.

## 2. Kafka integration

### Event flow

1. Producer accepts POST /api/v1/activities.
2. Producer converts the request into a `UserActivity` payload.
3. Kafka topic `user-activity` receives the event.
4. Consumer group `activity-processor` reads the message.
5. Repository writes durable processed records to MongoDB.

### Delivery semantics

- At-least-once is the baseline design for Kafka consumers.
- The processor uses a business idempotency key (`eventId`) to ensure duplicate messages do not create duplicate records.
- Retries use exponential backoff with bounded retries and dead-letter-topic or retry-topic strategy for unrecoverable messages.

### Failure handling

- Producer failure: return 503 and keep retry logic on the client side.
- Kafka outage: fail fast for write requests; use queue backpressure and a retry queue.
- Consumer failure: process exceptions trigger retry; duplicate message handling is guarded by `eventId`.

## 3. Data persistence and MongoDB

### Schema shape

```js
const activityLogSchema = new mongoose.Schema({
  eventId: { type: String, required: true, unique: true, index: true },
  userId: { type: String, required: true, index: true },
  activityType: { type: String, required: true, index: true },
  timestamp: { type: Date, required: true, index: true },
  source: { type: String, default: 'web' },
  metadata: { type: Object, default: {} },
  status: { type: String, default: 'processed' },
  expiresAt: { type: Date, required: true, index: { expireAfterSeconds: 0 } }
}, { timestamps: true });
```

### Index strategy

- userId
- activityType
- timestamp
- compound index: `{ userId: 1, activityType: 1, timestamp: -1 }`
- TTL index on `expiresAt` for retention/cleanup

### Archival strategy

- Keep hot data for 30-90 days in MongoDB.
- Archive older records into cold storage, e.g., Parquet on GCS/S3 or a data lake.
- Use scheduled exports or a daily ETL pipeline.

## 4. API design

### REST endpoints

- POST /api/v1/activities
- GET /api/v1/activity-logs?page=1&size=20&userId=123&activityType=login&startDate=2026-01-01&endDate=2026-01-31&sort=-timestamp&fields=userId,activityType,timestamp
- GET /health

### Query semantics

- page: page number, default 1
- size: page size, default 20, capped at 100
- filters: userId, activityType, startDate, endDate
- sort: comma-delimited field names with optional `-` prefix
- fields: comma-delimited projection list

### Security notes

- Add auth middleware for production using JWT or mTLS.
- Use `helmet`, `express-rate-limit`, and strict CORS policy.
- Validate all incoming payloads with Joi.

## 5. Deployment and orchestration

### Docker and local dev

Run the project locally with:

```bash
npm install
npm run docker:up
npm run api
npm run processor
npm run producer
```

The Docker compose stack starts Kafka, Zookeeper, and MongoDB. The app services are then built and run locally with environment variables from `.env`.

### Kubernetes

The project includes manifests under `k8s/` for:

- Namespace
- ConfigMap
- Secret
- Deployments
- Services

### Cloud options

- GKE: best if you want Google-managed autoscaling and managed Kafka via Confluent or a managed queue.
- EKS: strong AWS ecosystem compatibility.
- AKS: good for Azure-native integrations and private networking.

Recommended baseline limits:

- API: 250m CPU / 256Mi memory, HPA target 70% CPU
- Processor: 500m CPU / 512Mi memory, HPA target 60% CPU
- MongoDB: 1Gi storage, persistent volume claims

## 6. NFRs and observability

### Observability

- Structured logs with JSON logging fields: timestamp, service, level, userId, correlationId.
- Distributed tracing using OpenTelemetry.
- Metrics via Prometheus exporters for Kafka lag, HTTP latency, DB query latency, and message processing errors.

### Health checks

- /health on all services
- readiness and liveness endpoints for k8s probes
- dead-letter monitoring and circuit breakers for Kafka producer failure

### Idempotency and exactly-once

- At least once is expected at the message layer.
- Exactly-once can be approximated with transactional writes and dedupe keys when using a single DB transaction per message. In practice, idempotent persistence is the most robust pattern.

### Testing strategy

- Unit tests for domain validation and mapping logic
- Integration tests for Kafka and MongoDB behavior
- Contract tests for API response shapes and pagination

## 7. Deliverables and file layout

```text
.
├── apps/
│   ├── activity-api/
│   │   ├── Dockerfile
│   │   └── src/
│   │       └── server.js
│   ├── activity-processor/
│   │   ├── Dockerfile
│   │   └── src/
│   │       ├── server.js
│   │       └── activity-consumer.js
│   └── activity-producer/
│       ├── Dockerfile
│       └── src/
│           └── server.js
├── libs/
│   ├── application/
│   │   └── src/
│   │       ├── activity-log-service.js
│   │       └── activity-query-service.js
│   ├── domain/
│   │   └── src/
│   │       ├── user-activity.js
│   │       └── activity-processed.js
│   └── infrastructure/
│       └── src/
│           ├── kafka/
│           │   └── kafka-client.js
│           └── mongo/
│               └── activity-log.repository.js
├── k8s/
│   ├── namespace.yaml
│   ├── configmap.yaml
│   ├── secret.yaml
│   ├── api-deployment.yaml
│   ├── processor-deployment.yaml
│   └── service.yaml
├── .env.example
├── .gitignore
├── docker-compose.yml
├── package.json
├── README.md
└── tests/
    └── user-activity.test.js
```

## 8. MVP and enhancements

### Phase 1: MVP

- Producer emits events to Kafka.
- Consumer persists to MongoDB.
- API reads activity logs with filtering and pagination.
- Basic health checks and Docker local setup.

### Phase 2: Enhancements

- Add dead-letter queue and retry topics.
- Add OpenTelemetry tracing and metrics.
- Add auth and RBAC.
- Add event schema registry and validation.
- Add streaming analytics and dashboarding.

## 9. Minimal runnable example

The repository includes executable code for:

- API service: read logs at GET /api/v1/activity-logs
- Producer service: POST /api/v1/activities
- Processor service: consumes events and writes to MongoDB

## 10. Notes on stack choices

- Node.js + Express is suitable for rapid service implementation and easy Kafka integration.
- Kafka is used for asynchronous, resilient event distribution.
- MongoDB is chosen because the workload is mostly event-based reads and flexible JSON documents.
- This stack is a solid MVP and can be upgraded with EventStore, Postgres, or a dedicated search layer later.
