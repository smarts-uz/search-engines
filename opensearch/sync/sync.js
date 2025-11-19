import { Client } from '@opensearch-project/opensearch';
import pkg from 'pg';
import { normalizeRow, loadLastSyncTimes, saveLastSyncTimes } from './utils.js';

const { Pool } = pkg;

const pgPool = new Pool({
  user: process.env.PG_USER,
  host: process.env.PG_HOST,
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  port: process.env.PG_PORT,
});

const osClient = new Client({
  node: process.env.OPENSEARCH_NODE || 'http://localhost:9200',
});

const TABLES_TO_SYNC = process.env.TABLES_TO_SYNC 
  ? process.env.TABLES_TO_SYNC.split(',').map(t => t.trim())
  : ["player", "club", "team", "user", "match", "player_result", "player_point"];

export async function incrementalSync() {
  const lastSyncTimes = loadLastSyncTimes(TABLES_TO_SYNC);

  try {
    for (const table of TABLES_TO_SYNC) {
      console.log(`🔄 ${table} jadvali incremental sync qilinmoqda...`);

      // Indexni yaratish (agar oldin mavjud bo'lsa - ignore)
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
              name: { type: 'text', analyzer: 'translit_analyzer',search_analyzer: 'translit_analyzer',fielddata:true},
              geo: { type: 'geo_point' },
              agent: { type: 'object' }
            }
          }
        }
      }, { ignore: [400] });

      // 1️⃣ Yangi yoki yangilangan qatorlarni olish
      const { rows: updatedRows } = await pgPool.query(
        `SELECT * FROM "${table}" 
         WHERE updated_at > $1 
           AND deleted_at IS NULL`,
        [lastSyncTimes[table]]
      );

      // SCHEMA aniqlash (bir marta)
      let schema = {};
      if (updatedRows.length > 0) {
        const sample = updatedRows[0];
        for (const key of Object.keys(sample)) {
          if (key === 'geo') schema[key] = 'geo_point';
          else if (key === 'agent') schema[key] = 'agent';
          else schema[key] = 'other';
        }
      }

      // 2️⃣ OpenSearchga insert/update
      for (const row of updatedRows) {
        const normalizedRow = normalizeRow(row, schema);
        await osClient.index({
          index: table,
          id: row.id,
          body: normalizedRow,
        });
      }

      if (updatedRows.length > 0)
        console.log(`✅ ${updatedRows.length} yozuv sync qilindi (${table}).`);
      else
        console.log(`ℹ ${table} jadvalida yangilangan yozuv yo‘q.`);

      // 3️⃣ DELETED qatorlarni topish
      const { rows: deletedRows } = await pgPool.query(
        `SELECT id FROM "${table}" 
         WHERE deleted_at IS NOT NULL
           AND deleted_at > $1`,
        [lastSyncTimes[table]]
      );

      // 4️⃣ OpenSearchdan DELETE qilish
      for (const d of deletedRows) {
        await osClient.delete({
          index: table,
          id: d.id,
        }).catch(() => {}); // Agar mavjud bo'lmasa error bermasin
      }

      if (deletedRows.length > 0)
        console.log(`🗑 ${deletedRows.length} ta yozuv OpenSearchdan o‘chirildi (${table}).`);

      // Sync time yangilash
      lastSyncTimes[table] = new Date();
    }

    // 5️⃣ Sync vaqtlarini saqlash
    saveLastSyncTimes(lastSyncTimes);

  } catch (err) {
    console.error('❌ Incremental sync xatolik:', err);
  }
}
