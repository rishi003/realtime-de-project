import cron from 'node-cron';
import { fingerprint } from './fingerprint/index.js';
import { redisClient } from './redis/index.js';
import { roomForRentScraper } from './scraper/index.js';

cron.schedule('*/10 * * * * *', () => {
  roomForRentScraper().then((res) => {
    res.map((ad) => {
      redisClient.set(ad.id, ad.url);
    });
  });
});
