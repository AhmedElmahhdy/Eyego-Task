import express from 'express';
import helmet from 'helmet';
import morgan from 'morgan';
import rateLimit from 'express-rate-limit';
import mongoose from 'mongoose';
import dotenv from 'dotenv';
import Joi from 'joi';
import { ActivityLogRepository } from '../../../libs/infrastructure/src/mongo/activity-log.repository.js';
import { ActivityLogService } from '../../../libs/application/src/activity-log-service.js';

dotenv.config();

const app = express();
const port = process.env.API_PORT || process.env.PORT;
const mongoUri = process.env.MONGODB_URI ;
console.log(mongoUri)
await mongoose.connect(mongoUri);


const repository = new ActivityLogRepository();
const service = new ActivityLogService(repository);

app.use(helmet());
app.use(morgan('tiny'));
app.use(express.json());
app.use(rateLimit({ windowMs: 60_000, max: 120 }));

const querySchema = Joi.object({
  page: Joi.number().integer().min(1).default(1),
  size: Joi.number().integer().min(1).max(100).default(20),
  userId: Joi.string().optional(),
  activityType: Joi.string().optional(),
  startDate: Joi.date().iso().optional(),
  endDate: Joi.date().iso().optional(),
  sort: Joi.string().optional(),
  fields: Joi.string().optional()
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'activity-api' });
});

app.get('/api/v1/activity-logs', async (req, res) => {
  try {
    const { value, error } = querySchema.validate(req.query, { convert: true });
    if (error) {
      return res.status(400).json({ message: error.message });
    }

    const result = await service.findLogs({
      userId: value.userId,
      activityType: value.activityType,
      startDate: value.startDate,
      endDate: value.endDate,
      page: value.page,
      size: value.size,
      sort: value.sort,
      fields: value.fields
    });

    res.json(result);
  } catch (error) {
    res.status(500).json({ message: 'Failed to query logs', error: error.message });
  }
});

app.listen(port, () => {
  console.log(`Activity API running on port ${port}`);
});
