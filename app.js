// import dependencies
// data
const { count } = require('console');
const allData = require('./data.js');

// Mongo
const { MongoClient } = require('mongodb');
const host = "127.0.0.1";
const port = 27017;

// -- main tasks starts here --
// task 0: connect to MongoDB, and specify a database name to be created
const dbName = "statsdb";
const client = new MongoClient(`mongodb://${host}:${port}/${dbName}`);

// function for task 1: create that database, and add a log
async function createDB(dbName) {
  try {
    await client.connect();
    const db = client.db(dbName);
    console.log(`Successfully created database ${dbName}!`);
    return db
  } catch (err) {
    console.error(`Failed to create database ${dbName} - ${err}`)
    client.close();
  }
}

// function for task 2: create a collection with a given name, and add a log
const censusCollName = "uscensus";
async function createColl(db, name){
  try {
    coll = await db.createCollection(name);
    console.log(`Successfully created collection ${name}!`);
    return coll
  } catch (err) {
    console.error(`Failed to create collection ${name} - ${err}`)
    client.close();
  }
}

// function for task 3 & 4: add given data to the collection, and add a log
async function addData(coll, data) {
  try {
    let res = await coll.insertMany(data);
    let count = res.insertedCount
    console.log(`Successfully inserted ${count} documents!`);
    return count;
  } catch (err) {
    console.error(`Failed to insert data - ${err}`);
    client.close();
  }
}

// function for task 5: get zip code by city and state, and add a log
async function getZipcodeByCityState(coll, city, state) {
  try {
    let query = { city, state }  // equivalent to {city: city, state: state}
    let doc = await coll.findOne(query);
    if (doc !== null) {
      console.log(`The zip code for ${city}, ${state} is ${doc.zip}.`);
      return doc.zip
    } else {
      console.log(`No documents found for ${city}, ${state} in getting zipcode.`);
      return null
    }
  } catch (err) {
    console.error(`Failed to get zipcode by city and state - ${err}`);
    client.close();
  }
}

// function for task 6: get the incomes for all cities in a state, and add a log
async function getCityIncomesByState(coll, state) {
  try {
    let query = { state }  // equivalent to {state: state}
    let projection = { city: 1, income: 1, _id: 0 };
    let results = await coll.find(query).project(projection).toArray();
    if (results.length !== 0) {
      console.log(`Found ${results.length} records for incomes in ${state}.`);
      console.log(results);
      return results
    } else {
      console.log(`No documents found for ${state} in getting city incomes.`);
      return null
    }
  } catch (err) {
    console.error(`Failed to get incomes of all cities in ${state} - ${err}`);
    client.close();
  }
}

// task 7: update the income and age for THE SINGLE Alaska Entry, and add a log
async function updateAlaska(coll, newIncome, newAge) {
  try {
    let query = { state: "AK" };
    let newValue = {$set: {income: newIncome, age: newAge}};
    let res = await coll.updateOne(query, newValue);
    console.log(`${res.modifiedCount} document updated for the Alaska entry.`);
    return res
  } catch (err) {
    console.error(`Failed to update the Alaska entry - ${err}`);
    client.close();
  }
}

// task 8: get all documents sorted by state in ascending order, and add a log
async function getSortedDocumentsByState(coll, ascending=true) {
  try {
    let project = {_id: 0};
    let sort = ascending ? {state: 1} : {state: -1};
    let sortedDocs = await coll.find().project(project).sort(sort).toArray();
    console.log(`Successfully sorted ${sortedDocs.length} documents by state.`);
    console.log(sortedDocs);
    return sortedDocs;
  } catch (err) {
    console.error(`Failed to get sorted documents by state - ${err}`);
    client.close();
  }
}

// -- run all tasks with inputs --
async function runAllTasks() {
  // Note: this function should be idempotent

  // task 1: create the database namd "statsdb"
  const statsDB = await createDB(dbName);

  // task 2: create a collection named "uscensus"
  await statsDB.collection(censusCollName).drop();  // for idempotency
  const censusColl = await createColl(statsDB, censusCollName);

  // task 3: insert 6 documents in `var stats` in the file `data.js`
  await addData(censusColl, allData.stats);
  console.log(`Total documents: ${await censusColl.countDocuments()}`);  // 6

  // task 4: insert 2 extra documents in `var stats_2` in the file `data.js`
  await addData(censusColl, allData.stats_2);
  console.log(`Total documents: ${await censusColl.countDocuments()}`);  // 8

  // task 5: get the zip code for Corona, NY
  await getZipcodeByCityState(censusColl, "Corona", "NY");

  // task 6: get the incomes for all cities in California
  await getCityIncomesByState(censusColl, "CA");

  // task 7: update the SINGLE Alaska entry with new value: income = "38910", age = "46"
  await updateAlaska(censusColl, newIncome="38910", newAge="46");

  // task 8: get all documents sorted by state in ascending order
  await getSortedDocumentsByState(censusColl);
}

// run the homework
runAllTasks();
