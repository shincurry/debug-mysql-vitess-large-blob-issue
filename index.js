const mysql = require('mysql2/promise');
const config = require('./config');

async function demo() {
  const connection = await mysql.createConnection({
    ...config.mysql,
    ssl: {
      rejectUnauthorized: true,
    },
  });

  await connection.query(`
    CREATE TABLE IF NOT EXISTS DebugDocument (
      name varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      data longblob NOT NULL,
      PRIMARY KEY (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
  `);

  const upsertDocument = async (size) => {
    const payload = {
      name: `test-large-blob-${size}`,
      data: (() => {
        // Create a (:size) MB buffer
        const bufferSize = size * 1024 * 1024; // (:size) MB in bytes
        const largeBuffer = Buffer.alloc(bufferSize);

        // Fill the buffer with some data (optional)
        largeBuffer.fill('A');

        return largeBuffer;
      })(),
    }

    try {
      const [result, fields] = await connection.query({
        sql: `
          INSERT INTO DebugDocument (name,data) VALUES (?,?) ON DUPLICATE KEY UPDATE name=VALUES(name),data=VALUES(data);
        `,
        values: [payload.name, payload.data],
      })
      console.log(`SUCCESS upsert on size: ${size}`)
      return true
    } catch (error) {
      console.error(`FAILED upsert on size: ${size}`)
      console.error(error)
      return false
    }

  }

  let leftSize = 1;
  let rightSize = 30;

  await upsertDocument(leftSize)
  await upsertDocument(rightSize)
  while (leftSize < rightSize) {
    const size = Math.floor((leftSize + rightSize) / 2)
    const success = await upsertDocument(size)
    if (success) {
      leftSize = size + 1;
    } else {
      rightSize = size - 1;
    }
  }
}

demo().catch((err) => console.error(err));