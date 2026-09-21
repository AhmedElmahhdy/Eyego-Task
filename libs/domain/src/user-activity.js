import { randomUUID } from 'node:crypto';

export class UserActivity {
  constructor({
    userId,
    activityType,
    source = 'web',
    metadata = {},
    timestamp = new Date(),
    eventId = randomUUID()
  }) {
    this.userId = userId;
    this.activityType = activityType;
    this.source = source;
    this.metadata = metadata;
    this.timestamp = timestamp;
    this.eventId = eventId;
  }

  toKafkaPayload() {
    const payload = {
      eventId: this.eventId,
      userId: this.userId,
      activityType: this.activityType,
      source: this.source,
      metadata: this.metadata,
      timestamp: this.timestamp.toISOString()
    };

    return {
      topic: 'user-activity',
      key: this.userId,
      value: JSON.stringify(payload)
    };
  }
}

export class ActivityProcessed {
  constructor({
    eventId,
    userId,
    activityType,
    source,
    metadata,
    timestamp,
    status = 'processed'
  }) {
    this.eventId = eventId;
    this.userId = userId;
    this.activityType = activityType;
    this.source = source;
    this.metadata = metadata;
    this.timestamp = new Date(timestamp);
    this.status = status;
  }
}
