const mysql = require('mysql2');
const admin = require('firebase-admin');
const fs = require('fs');

const serviceAccount = require('./digital-world-logbook-dev-firebase-adminsdk-f3ltf-3e093b3e7e.json');
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();


const connection = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'fcc_amateur',
});

const query = `
  SELECT 
    en.fccid, en.callsign, en.full_name, en.first, en.middle, en.last, en.address1, 
    en.city, en.state, en.zip, 
    hd.status, 
    am.class, am.col4, am.col5, am.col6, am.former_call, am.former_class
  FROM en
  LEFT JOIN hd ON en.fccid = hd.fccid
  LEFT JOIN am ON en.fccid = am.fccid
  LIMIT ?, 5000;  -- Pagination using OFFSET
`;

async function uploadData(offset = 0) {
  try {
    const batchSize = 5000;
    let batchCount = 0;
    let batch = db.batch();
    const collectionRef = db.collection('FCCAmateur_Uzair');

    const queryWithOffset = mysql.format(query, [offset]);

    const stream = connection.query(queryWithOffset).stream();

    for await (const row of stream) {
      const docRef = collectionRef.doc();

      const data = {
        address1: row.address1 || '',
        callsign: row.callsign,
        city: row.city || '',
        class: row.class || '',
        col4: row.col4 || '',
        col5: row.col5 || '',
        col6: row.col6 || '',
        fccid: row.fccid,
        first: row.first || '',
        former_call: row.former_call || '',
        former_class: row.former_class || '',
        full_name: row.full_name || '',
        last: row.last || '',
        middle: row.middle || '',
        state: row.state || '',
        status: row.status || '',
        zip: row.zip || '',
      };

      batch.set(docRef, data);
      batchCount++;

      if (batchCount >= batchSize) {
        await commitBatch(batch);  
        batch = db.batch();
        batchCount = 0;
      }
    }

    if (batchCount > 0) {
      await commitBatch(batch);  
    }
    connection.query('SELECT COUNT(*) AS count FROM en', (err, results) => {
      if (err) {
        console.error('Error counting total records:', err);
        connection.end();
        return;
      }

      const totalRecords = results[0].count;
      if (offset + 5000 < totalRecords) {
        console.log(`Fetching next batch of records starting from offset ${offset + 5000}`);
        uploadData(offset + 5000); 
      } else {
        console.log('All records uploaded successfully.');
        connection.end(); 
      }
    });

  } catch (error) {
    console.error('Error during upload:', error);
    connection.end(); 
  }
}


async function commitBatch(batch) {
  try {
    await batch.commit();
    console.log('Batch uploaded successfully to Firebase.');
  } catch (err) {
    console.error('Error committing batch to Firestore:', err);
  }
}

uploadData();  














// 5v28z14FzLVIyOmQjf4m