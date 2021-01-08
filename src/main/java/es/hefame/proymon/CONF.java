package es.hefame.proymon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CONF {
	
	private static Logger L = LogManager.getLogger();

	public static String mongoConnectionString() {
		String mongoString =  System.getProperty("es.hefame.proymon.mongo");
		
		if (mongoString == null) {
			L.warn("Se devuelve la cadena de conexión a mongoDB por defecto");
			return "";
		}
		return mongoString;
	}
	
	
	public static boolean debug() {
		String debugMode =  System.getProperty("es.hefame.proymon.debug");
		return "true".equalsIgnoreCase(debugMode);
	}
	
	
	

}

