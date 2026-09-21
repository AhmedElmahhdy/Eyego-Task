import express from 'express';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import morgan from 'morgan';
import dotenv from 'dotenv';
import Joi from 'joi';
import { createKafkaClient } from '../../../libs/infrastructure/src/kafka/kafka-client.js';
import { UserActivity } from '../../../libs/domain/src/user-activity.js';

dotenv.config();

const app = express();
const port = Number(process.env.PRODUCER_PORT);

app.use(helmet());
app.use(morgan('tiny'));
app.use(express.json());
app.use(rateLimit({ windowMs: 60_000, max: 60 }));

const schema = Joi.object({
  userId: Joi.string().required(),
  activityType: Joi.string().required(),
  source: Joi.string().default('web'),
  metadata: Joi.object().default({})
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'activity-producer' });
});

app.post('/api/v1/activities', async (req, res) => {
  try {
    const { value, error } = schema.validate(req.body);
    if (error) {
      return res.status(400).json({ message: error.message });
    }

    const producer = createKafkaClient({ brokers: process.env.KAFKA_BROKERS || 'localhost:9092' });
    const kafkaProducer = producer.producer();
    await kafkaProducer.connect();

    const activityEvent = new UserActivity(value);
    const message = activityEvent.toKafkaPayload();

    await kafkaProducer.send({
      topic: message.topic,
      messages: [{ key: message.key, value: message.value }]
    });

    await kafkaProducer.disconnect();

    res.status(202).json({
      message: 'activity accepted',
      eventId: activityEvent.eventId,
      activityType: activityEvent.activityType
    });
  } catch (err) {
    res.status(503).json({ message: 'Kafka unavailable', error: err.message });
  }
});

app.listen(port, () => {
  console.log(`Activity producer running on port ${port}`);
});
