import cron from 'node-cron';
import { incrementalSync } from './sync.js';

console.log('⏰ Sync service ishga tushdi');

// Har 5 daqiqada sync ishga tushadi
cron.schedule('*/2 * * * *', async () => {
  console.log('⏳ Sync ishga tushdi:', new Date());
    await incrementalSync();

});
