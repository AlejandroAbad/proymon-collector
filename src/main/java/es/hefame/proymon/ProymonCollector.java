package es.hefame.proymon;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;

import es.hefame.proymon.consultasXML.ConsultasXML;
import es.hefame.proymon.labware.LabSap;
import es.hefame.proymon.pedidos.ImpresoraLine;
import es.hefame.proymon.pedidos.ImpresoraReader;
import es.hefame.proymon.pedidos.ImpresoraLine.TipoLinea;

public class ProymonCollector {
	
	private static Logger L = LogManager.getLogger();

	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			operate();
		} else {
			String nombreFichero = args[0];
			File file = new File(nombreFichero);
			cargarDesdeFichero(file);
		}
	}

	
	private static void cargarDesdeFichero(File file) throws IOException {
		L.info("Cargando entradas de Proyman desde el fichero: {}", file.getAbsoluteFile());
		
		FileReader file_reader = null;
		BufferedReader buffer = null;
		
		try {
			L.info("Abriendo el fichero [{}].", file.getAbsoluteFile());
			file_reader = new FileReader(file);
			buffer = new BufferedReader(file_reader);
		} catch (IOException e) {
			L.error("Ocurrio una excepcion al abrir el fichero.");
			L.catching(e);
			throw new IOException("Error al abrir el fichero de log", e);
		}
		
		
		MongoCollection<Document> pedidos_collection = MongoProvider.getCollection("proyman", "pedidos");

		String line = null;
		while ((line = buffer.readLine()) != null) {

			ImpresoraLine l = new ImpresoraLine(line);
			try {
				if (l.tipo != TipoLinea.DISCARD) {
					if (l.tipo == TipoLinea.CHEQUEO_OK)
						pedidos_collection.deleteOne(l.getDocumentFilter());
					pedidos_collection.updateOne(l.getDocumentFilter(), l.getDocumentForUpdate(), new UpdateOptions().upsert(true));
				}
				
			} catch (MongoException ex) {
				L.error("Ocurrio una excepci�n al conectarse con la base de datos");
				L.catching(ex);
			}
			
		}
		
		if (file_reader != null) file_reader.close();
		if (buffer != null) buffer.close();
		
		
	}
	
	
	private static void operate() throws IOException {

		L.info("Inciado recolector Proymon");
		
		ImpresoraReader impresoraReader = new ImpresoraReader();
		ConsultasXML albaranXmlReader = new ConsultasXML();
		LabSap labsapReader = new LabSap();

		while (!Thread.interrupted()) {
			try {
				labsapReader.operate();
				albaranXmlReader.operate();
				impresoraReader.operate();
				Thread.sleep(5000);
			} catch (InterruptedException e) {

			}
		}
		MongoProvider.close();
	}

}
