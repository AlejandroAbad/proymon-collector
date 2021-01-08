package es.hefame.proymon.consultasXML;

import java.io.File;
import java.io.IOException;

import org.bson.BsonDateTime;
import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;

import es.hefame.proymon.CONF;
import es.hefame.proymon.MongoProvider;

public class ConsultasXML {


	private File file;

	public ConsultasXML() {
		this.file = new File("/tmp/impresora2");
		if (CONF.debug()) this.file = new File("C:\\impresora2");
	}


	public void operate() throws IOException {
		MongoCollection<Document> controlCollection = MongoProvider.getCollection("proyman", "control");

		Document query = new Document();
		query.put("_id", "ConsultasXML");

		Document update = new Document();
		Document set = new Document();
		UpdateOptions options = new UpdateOptions();
		options.upsert(true);

		update.put("$set", set);
		set.put("_id", "ConsultasXML");
		set.put("timestamp", new BsonDateTime(file.lastModified()));

		controlCollection.updateOne(query, update, options);
	}

	


}
