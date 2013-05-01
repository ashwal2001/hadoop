package com.xyz.reccommendation.mongo;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * Java + MongoDB Hello world Example
 * 
 */
public class MongoDBTest {
	public static void main(String[] args) {

		try {

			String envt = null;

			if (args.length > 0) {
				envt = args[0];
			} else {
				envt = "dev";
			}

			Properties prop = new Properties();

			try {
				// load a properties file from class path, inside static method
				prop.load(MongoDBTest.class.getClassLoader()
						.getResourceAsStream("config-" + envt + ".properties"));

			} catch (IOException ex) {
				ex.printStackTrace();
				System.exit(1);
			}

			/**** Connect to MongoDB ****/
			// Since 2.10.0, uses MongoClient
			MongoClient mongo = new MongoClient(prop.getProperty("mongodb.ip"),
					27017);

			/**** Get database ****/
			// if database doesn't exists, MongoDB will create it for you
			DB db = mongo.getDB(prop.getProperty("mongodb.dbname")); // testdb

			/**** Get collection / table from 'testdb' ****/
			// if collection doesn't exists, MongoDB will create it for you
			DBCollection table = db.getCollection(prop
					.getProperty("mongodb.collectionname.input"));

			/**** Find and display ****/
			BasicDBObject searchQuery = new BasicDBObject();
			searchQuery.put("sessionId", "3h60qf27tgvrppg635o84ftaf3");

			DBCursor cursor = table.find(searchQuery);

			while (cursor.hasNext()) {
				System.out.println(cursor.next());
			}

			/**** Done ****/
			System.out.println("Done");

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}

	}
}