import { createClient } from 'redis';

export const redisClient = createClient({ url: 'redis://localhost:6379' });

redisClient.connect();
redisClient.on('error', (err) => {});
redisClient.on('connect', () => console.log('Redis Client Connected'));
// redisClient.on('reconnecting', () => console.log('Redis Client Reconnecting'));
