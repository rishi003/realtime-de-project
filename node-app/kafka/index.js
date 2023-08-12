import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'node-app',
  brokers: ['localhost:9092'],
});

export const kafkaProducer = kafka.producer();

kafkaProducer.connect().then(() => {
  console.log('Producer connected');
});

kafkaProducer.disconnect().then(() => {
  console.log('Producer disconnected');
});
