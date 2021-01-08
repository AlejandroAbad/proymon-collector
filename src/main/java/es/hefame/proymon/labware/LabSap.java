package es.hefame.proymon.labware;

import java.io.File;
import java.io.FileFilter;
import java.util.HashMap;

import org.bson.BsonDateTime;
import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;

import es.hefame.proymon.CONF;
import es.hefame.proymon.MongoProvider;

public class LabSap {

	
	private static HashMap<String, DirStatus> cache = new HashMap<String, DirStatus>(3);
	private static HashMap<String, Long> timeCache = new HashMap<String, Long>(3);
	
	public void operate() {
		
		LabSapResult result = new LabSapResult(
				getDirStatus("/proyman/isap/dat/labsap"),
				getDirStatus("/proyman/isap/dat/saplab")
			);
		
		if (CONF.debug()) {
			result = new LabSapResult(
				getDirStatus("D:\\DOCS"),
				new DirStatus(0, 0) 
			);
		} 

		MongoCollection<Document> controlCollection = MongoProvider.getCollection("proyman", "control");

		Document query = new Document();
		query.put("_id", "Labware");

		Document update = new Document();
		update.put("$set", result.toDocument());
		UpdateOptions options = new UpdateOptions();
		options.upsert(true);

		controlCollection.updateOne(query, update, options);

	}
	
	
	private DirStatus getDirStatus(String dir) {
		
			
		if (cache.containsKey(dir) && timeCache.containsKey(dir) && (System.currentTimeMillis() - timeCache.get(dir)) < 5000) {
			return cache.get(dir);
		}
		
	    File fl = new File(dir);
	    File[] files = fl.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.isFile();
			}
		});
	    
	    long firstMod = Long.MAX_VALUE;
	    if (files != null) {
		    for (File file : files) {
		        if (file.lastModified() < firstMod) {
		        	firstMod = file.lastModified();
		        }
		    }
		    cache.put(dir, new DirStatus(files.length, firstMod));
		    timeCache.put(dir, System.currentTimeMillis());
	    }
	    
	    
	    return cache.get(dir);
	    
	}
	
	
	private class DirStatus {
		public final int numFiles;
		public final long lastMod;
		public DirStatus(int numFiles, long lastMod) {
			this.numFiles = numFiles;
			this.lastMod = lastMod;
		}
		
		
		public Document toDocument() {
			Document root = new Document();
			root.put("num", numFiles);
			root.put("last", lastMod);
			return root;
		}
	}
	
	private class LabSapResult {
		public final DirStatus labsap;
		public final DirStatus saplab;
		public LabSapResult(DirStatus labsap, DirStatus saplab) {
			this.labsap = labsap;
			this.saplab = saplab;
		}

		public Document toDocument() {
			Document root = new Document();
			root.put("_id", "Labware");
			root.put("labsap", labsap.toDocument());
			root.put("saplab", saplab.toDocument());
			root.put("timestamp", new BsonDateTime(System.currentTimeMillis()));
			return root;
		}
		
	}

}
