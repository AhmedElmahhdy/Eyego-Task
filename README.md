# Eyego Activity Stream

This project is a small event-driven platform for capturing, processing, and querying user activity events. It demonstrates how a producer service can accept activity events, publish them to Kafka, let a processor service store them in MongoDB, and expose a read API for querying those records.

The purpose of the task is to show a simple but realistic flow:

1. A client sends a user activity event.
2. The producer validates and publishes it to Kafka.
3. The processor consumes the Kafka message.
4. The processor saves a processed activity log to MongoDB.
5. The API returns activity records with filtering, sorting, and pagination.

---

## Project goal

The repository models a minimal activity stream system using:

- Node.js
- Express
- Kafka
- MongoDB
- Docker Compose

It is structured around a simple layered design inspired by Domain-Driven Design.

---

## Architecture overview

```text
Client / Browser / App
        |
        v
activity-producer
        |
        | POST /api/v1/activities
        v
Kafka topic: user-activity
        |
        v
activity-processor
        |
        v
MongoDB collection: activitylogs
        |
        v
activity-api
        |
        | GET /api/v1/activity-logs
        v
Dashboard / Audit UI / Consumers
```

### Components

- activity-producer:
  - accepts HTTP requests
  - validates input
  - creates a `UserActivity` event
  - sends it to Kafka

- activity-processor:
  - subscribes to `user-activity`
  - prevents duplicates using `eventId`
  - writes processed records to MongoDB

- activity-api:
  - exposes query endpoints
  - reads activity data from MongoDB
  - supports filtering, pagination, and sorting

---

## Domain concepts

### UserActivity
This is the event emitted by the client or producer when a user performs an action such as login, purchase, page view, or logout.

### ActivityLog
This is the saved record in MongoDB after the processor consumes the event and stores it.

### Idempotency
The system uses `eventId` to avoid inserting the same event twice. The processor checks whether the event already exists before creating a new DB record.

---

## Project structure

```text
.
├── apps/
│   ├── activity-api/
│   │   └── src/
│   │       └── server.js
│   ├── activity-processor/
│   │   └── src/
│   │       └── server.js
│   └── activity-producer/
│       └── src/
│           └── server.js
├── libs/
│   ├── application/
│   │   └── src/
│   │       └── activity-log-service.js
│   ├── domain/
│   │   └── src/
│   │       └── user-activity.js
│   └── infrastructure/
│       └── src/
│           ├── kafka/
│           │   └── kafka-client.js
│           └── mongo/
│               └── activity-log.repository.js
├── k8s/
│   ├── api-deployment.yaml
│   ├── configmap.yaml
│   ├── namespace.yaml
│   ├── processor-deployment.yaml
│   ├── secret.yaml
│   └── service.yaml
├── docker-compose.yml
├── package.json
├── README.md
└── .gitignore
```

---

## Prerequisites

Before running the project, make sure you have:

- Node.js 18+
- npm
- Docker and Docker Compose
- Access to ports 4001, 4002, 4003, 9092, and 27017

---

## Environment variables

The services read environment variables from the shell or Docker Compose environment.

For local manual execution, use values like:

```bash
export KAFKA_BROKERS=localhost:9092
export MONGODB_URI=mongodb://admin:password@localhost:27017/activity_platform?authSource=admin
export API_PORT=3001
export PRODUCER_PORT=3002
export PROCESSOR_PORT=3003
```

For Docker Compose, these values are already configured inside `docker-compose.yml`.

---

## Quick start with Docker Compose

From the project root, run:

```bash
npm install
npm run docker:up
```

This starts:

- Kafka
- MongoDB
- Activity producer
- Activity processor
- Activity API

To stop everything:

```bash
npm run docker:down
```

---

## Running the services manually

If you want to run each service individually, open separate terminals and use:

### 1) Start the producer

```bash
export KAFKA_BROKERS=localhost:9092
export PRODUCER_PORT=3002
npm run producer
```

### 2) Start the processor

```bash
export KAFKA_BROKERS=localhost:9092
export MONGODB_URI=mongodb://admin:password@localhost:27017/activity_platform?authSource=admin
export PROCESSOR_PORT=3003
npm run processor
```

### 3) Start the API

```bash
export MONGODB_URI=mongodb://admin:password@localhost:27017/activity_platform?authSource=admin
export API_PORT=3001
npm run api
```

> The Kafka broker and MongoDB must be running before the processor and API start successfully.

---

## Health checks

Each service exposes a basic health endpoint:

- Producer: `GET /health`
- Processor: `GET /health`
- API: `GET /health`

Example:

```bash
curl http://localhost:3001/health
curl http://localhost:3002/health
curl http://localhost:3003/health
```

---

## API usage

### 1) Create an activity event

Endpoint:

```http
POST /api/v1/activities
```

Example request:

```bash
curl -X POST http://localhost:3002/api/v1/activities \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user-123",
    "activityType": "login",
    "source": "web",
    "metadata": {
      "ip": "127.0.0.1",
      "device": "desktop"
    }
  }'
```

Example success response:

```json
{
  "message": "activity accepted",
  "eventId": "7b0f...",
  "activityType": "login"
}
```

This request is validated by Joi and then sent to Kafka.

### 2) Query activity logs

Endpoint:

```http
GET /api/v1/activity-logs
```

Example request:

```bash
curl "http://localhost:3001/api/v1/activity-logs?page=1&size=20&userId=user-123&activityType=login&sort=-timestamp"
```

Optional filters:

- `page` – page number, default `1`
- `size` – page size, default `20`, max `100`
- `userId` – filter by user ID
- `activityType` – filter by activity type
- `startDate` – ISO date to filter from
- `endDate` – ISO date to filter until
- `sort` – such as `-timestamp` or `userId`
- `fields` – comma-separated field list

Example:

```bash
curl "http://localhost:3001/api/v1/activity-logs?userId=user-123&activityType=login&startDate=2026-01-01&endDate=2026-12-31&sort=-timestamp&fields=userId,activityType,timestamp"
```

---

## Kafka and MongoDB behavior

### Kafka flow

The producer sends messages to the `user-activity` topic. The processor consumes from the same topic using a consumer group named `activity-processor`.

### MongoDB storage

Each processed record contains:

- `eventId`
- `userId`
- `activityType`
- `timestamp`
- `source`
- `metadata`
- `status`
- `expiresAt`

The repository also creates indexes to improve querying speed, including:

- user ID
- activity type
- timestamp
- compound query index on `userId + activityType + timestamp`

---

## Notes about the implementation

This project is intentionally a lightweight MVP, not a production-grade platform. It is designed to demonstrate the full flow of:

- event ingestion
- Kafka-based messaging
- consumer-side processing
- persistence in MongoDB
- read API and query support

It includes basic validation and idempotency checks, but a production system would normally add:

- authentication and authorization
- retries and DLQ handling
- OpenTelemetry tracing
- proper observability and metrics
- more robust configuration management

---

## Useful commands

```bash
npm install
npm run docker:up
npm run docker:down
npm run producer
npm run processor
npm run api
```

---

## Summary

This task is a working example of a small event-driven activity pipeline. It shows how an application can accept events, publish them to Kafka, process them asynchronously, store them in MongoDB, and expose them through a query API.

If you want to test the flow end-to-end, run Docker Compose, send a `POST` to the producer, and then query the API to see the saved activity log.
