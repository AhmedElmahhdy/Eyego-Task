export class ActivityLogService {
  constructor(repository) {
    this.repository = repository;
  }

  async processMessage(message) {
    const payload = JSON.parse(message.value.toString());

    const existing = await this.repository.findByEventId(payload.eventId);
    if (existing) {
      return { inserted: false, reason: 'duplicate' };
    }

    const result = await this.repository.insert({
      eventId: payload.eventId,
      userId: payload.userId,
      activityType: payload.activityType,
      source: payload.source,
      metadata: payload.metadata,
      timestamp: new Date(payload.timestamp),
      status: 'processed',
      expiresAt: new Date(Date.now() + 90 * 24 * 60 * 60 * 1000)
    });

    return { inserted: true, result };
  }

  async findLogs(query) {
    return this.repository.findMany(query);
  }
}
