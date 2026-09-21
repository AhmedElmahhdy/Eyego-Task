import mongoose from 'mongoose';

const activityLogSchema = new mongoose.Schema(
  {
    eventId: { type: String, required: true, unique: true, index: true },
    userId: { type: String, required: true, index: true },
    activityType: { type: String, required: true, index: true },
    timestamp: { type: Date, required: true, index: true },
    source: { type: String, default: 'web' },
    status: { type: String, default: 'processed' },
    metadata: { type: Object, default: {} },
    expiresAt: { type: Date, required: true, index: { expireAfterSeconds: 0 } }
  },
  { timestamps: true }
);

activityLogSchema.index({ userId: 1, activityType: 1, timestamp: -1 });

export const ActivityLogModel = mongoose.model('ActivityLog', activityLogSchema);

export class ActivityLogRepository {
  constructor(model = ActivityLogModel) {
    this.model = model;
  }

  async findByEventId(eventId) {
    return this.model.findOne({ eventId }).lean();
  }

  async insert(data) {
    return this.model.create(data);
  }

  async findMany({
    userId,
    activityType,
    startDate,
    endDate,
    page = 1,
    size = 20,
    sort = '-timestamp',
    fields = ''
  }) {
    const filter = {};

    if (userId) filter.userId = userId;
    if (activityType) filter.activityType = activityType;
    if (startDate || endDate) {
      filter.timestamp = {};
      if (startDate) filter.timestamp.$gte = new Date(startDate);
      if (endDate) filter.timestamp.$lte = new Date(endDate);
    }

    const projection = fields
      .split(',')
      .map((field) => field.trim())
      .filter(Boolean)
      .reduce((acc, field) => {
        acc[field] = 1;
        return acc;
      }, {});

    const sortQuery = {};
    for (const part of (sort || '').split(',')) {
      const item = part.trim();
      if (!item) continue;
      const desc = item.startsWith('-');
      const key = desc ? item.slice(1) : item;
      sortQuery[key] = desc ? -1 : 1;
    }

    const limit = Math.min(Number(size) || 20, 100);
    const skip = (Math.max(Number(page) || 1, 1) - 1) * limit;

    const [items, total] = await Promise.all([
      this.model.find(filter, Object.keys(projection).length ? projection : null)
        .sort(sortQuery)
        .skip(skip)
        .limit(limit)
        .lean(),
      this.model.countDocuments(filter)
    ]);

    return {
      items,
      page: Number(page) || 1,
      size: limit,
      total,
      totalPages: Math.ceil(total / limit) || 1
    };
  }
}
