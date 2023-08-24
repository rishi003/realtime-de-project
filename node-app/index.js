import cron from 'node-cron';
import { fingerprint } from './fingerprint/index.js';
import { redisClient } from './redis/index.js';
import { roomForRentScraper, scrapeAd } from './scraper/index.js';
import { kafkaProducer } from './kafka/index.js';
import stableStringify from 'json-stable-stringify';
import dotenv from 'dotenv';
import findUp from 'find-up';

dotenv.config({ path: findUp.sync('.env') });

cron.schedule('*/15 * * * * *', () => {
  roomForRentScraper().then((res) => {
    const month = new Date().getMonth();
    res.map(async (ad) => {
      kafkaProducer.send({
        topic: 'room-for-rent',
        messages: [{ key: month.toString(), value: stableStringify(ad) }],
      });
      // if (!(await redisClient.get(`${month}-${ad.id}`))) {
      //   redisClient.set(`${month}-${ad.id}`, fingerprint(ad));
        
      // }
    });
  });
});

// cron.schedule('*/10 * * * * *', async () => {
//   const month = new Date().getMonth() - 3;
//   await redisClient.keys(`${month}-*`).then((keys) => {
//     keys.forEach((key) => {
//       redisClient.del(key);
//     });
//   });

//   const keys = await redisClient.keys(`*`);

//   for (let key of keys) {
//     const currentFingerprint = await redisClient.get(key);
//     const adUpdate = await scrapeAd(key.split('-')[1]);
//     const newFingerprint = fingerprint(adUpdate);
//     if (currentFingerprint !== newFingerprint) {
//       redisClient.set(key, newFingerprint);
//       kafkaProducer.send({
//         topic: 'room-for-rent-update',
//         messages: [{ key: key, value: stableStringify(adUpdate) }],
//       });
//     }
//   }
// });
