package es.hefame.proymon;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.bson.Document;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoProvider
{
	private static final Logger		L						= LogManager.getLogger();
	private static final String		MONGO_BASE_URL			;
	private static MongoClient		mongo_client			= null;


	static
	{
		MONGO_BASE_URL = CONF.mongoConnectionString();
	}

	public static MongoClient getClient()
	{
		if (mongo_client != null)
		{
			return mongo_client;
		}

		L.info("Creando conexión a MongoDB para operaciones del concentrador");
		
		mongo_client = MongoClients.create(MONGO_BASE_URL);
		return mongo_client;
	}

	public static void close() {
		if (mongo_client != null)
		{
			try {
				mongo_client.close();
			} catch (Exception mex) {
				
			}
			mongo_client = null;
		}
	}
	
	public static MongoDatabase getDatabase(String database)
	{
		MongoClient client = MongoProvider.getClient();
		if (client == null) return null;
		return client.getDatabase(database);
	}

	public static MongoCollection<Document> getCollection(String database, String collection)
	{
		MongoDatabase db = MongoProvider.getDatabase(database);
		if (db == null) return null;
		return db.getCollection(collection);
	}

	public static <T extends Document> MongoCollection<T> getCollection(String database, String collection, Class<T> document_class)
	{
		MongoDatabase db = MongoProvider.getDatabase(database);
		if (db == null) return null;
		return db.getCollection(collection, document_class);
	}

}
