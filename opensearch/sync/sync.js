import { Client } from '@opensearch-project/opensearch';
import pkg from 'pg';
const { Pool } = pkg;

const pgPool = new Pool({
  user: process.env.PGUSER,
  host: process.env.PGHOST,
  database: process.env.PGDATABASE,
  password: process.env.PGPASSWORD,
  port: process.env.PGPORT,
});

const osClient = new Client({
  node: process.env.OPENSEARCH_NODE || 'http://opensearch:9200',
});

// Read tables to sync from environment variable, fallback to default if not provided
const TABLES_TO_SYNC = process.env.TABLES_TO_SYNC 
  ? process.env.TABLES_TO_SYNC.split(',').map(table => table.trim())
  : ['player', 'club', 'team'];

// Oxirgi sync vaqtini saqlash (yoki fayl/db)
let lastSyncTime = new Date(0); // dastlab 1970

export async function incrementalSync() {
  try {
    for (const table of TABLES_TO_SYNC) {
      console.log(`🔄 ${table} jadvali incremental sync qilinmoqda...`);

      // 1️⃣ Indeks mavjud bo‘lmasa, yaratamiz translit analyzer bilan
      await osClient.indices.create({
        index: table,
        body: {
          settings: {
            analysis: {
              analyzer: {
                translit_analyzer: {
                  tokenizer: 'standard',
                  filter: ['lowercase', 'russian_translit']
                }
              },
              filter: {
                russian_translit: {
                  type: 'icu_transform',
                  id: 'Any-Latin; Latin-Cyrillic'
                }
              }
            }
          },
          mappings: {
            properties: {
              name: {
                type: 'text',
                analyzer: 'translit_analyzer',
                search_analyzer: 'translit_analyzer'
              }
              // agar boshqa text maydonlar ham translit kerak bo‘lsa, shu yerga qo‘shing
            }
          }
        }
      }, { ignore: [400] }); // 400 = index already exists

      // 2️⃣ Faqat o‘zgargan yoki yangi yozuvlarni olamiz
      const { rows } = await pgPool.query(
        `SELECT * FROM "${table}" WHERE updated_at > $1`,
        [lastSyncTime]
      );

      // 3️⃣ Hujjatlarni indexlash
      for (const row of rows) {
        await osClient.index({
          index: table,
          id: row.id, // optional: primary key bilan collision oldini olamiz
          body: row,
        });
      }

      console.log(`✅ ${rows.length} yozuv ${table} dan OpenSearch ga yuklandi.`);
    }

    lastSyncTime = new Date(); // oxirgi sync vaqtini yangilaymiz
  } catch (err) {
    console.error('❌ Incremental sync xatolik:', err);
  }
}
