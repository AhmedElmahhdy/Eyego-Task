import dotenv from 'dotenv';
import mongoose from 'mongoose';
import { createKafkaClient } from '../../../libs/infrastructure/src/kafka/kafka-client.js';
import { ActivityLogService } from '../../../libs/application/src/activity-log-service.js';
import { ActivityLogRepository } from '../../../libs/infrastructure/src/mongo/activity-log.repository.js';

dotenv.config();

const mongoUri = process.env.MONGODB_URI || 'mongodb://admin:password@localhost:27017/activity_platform?authSource=admin';
const topic = process.env.KAFKA_TOPIC_USER_ACTIVITY || 'user-activity';
const groupId = process.env.KAFKA_CONSUMER_GROUP || 'activity-processor';

await mongoose.connect(mongoUri);
console.log('Mongo connected');

const repository = new ActivityLogRepository();
const service = new ActivityLogService(repository);
const kafka = createKafkaClient({ brokers: process.env.KAFKA_BROKERS || 'localhost:9092', clientId: 'activity-processor' });
const consumer = kafka.consumer({ groupId });

await consumer.connect();
await consumer.subscribe({ topic, fromBeginning: false });

await consumer.run({
  eachMessage: async ({ message }) => {
    try {
      const result = await service.processMessage(message);
      console.log('Processed message', result);
    } catch (error) {
      console.error('Consumed message failed', error);
      throw error;
    }
  }
});

const port = Number(process.env.PROCESSOR_PORT || process.env.PORT || 4003);
const health = async (req, res) => {
  res.json({ status: 'ok', service: 'activity-processor' });
};

const express = (await import('express')).default;
const app = express();
app.get('/health', health);
app.listen(port, () => console.log(`Activity processor health on ${port}`));
