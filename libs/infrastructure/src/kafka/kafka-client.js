import { Kafka } from 'kafkajs';

export function createKafkaClient({ brokers, clientId = 'eyego-activity' }) {
  return new Kafka({
    clientId,
    brokers: brokers.split(',').map((value) => value.trim())
  });
}
