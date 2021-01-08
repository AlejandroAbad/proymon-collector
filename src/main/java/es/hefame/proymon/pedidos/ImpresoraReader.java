package es.hefame.proymon.pedidos;

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

import es.hefame.proymon.CONF;
import es.hefame.proymon.pedidos.ImpresoraLine.TipoLinea;
import es.hefame.proymon.MongoProvider;

public class ImpresoraReader {

	private static Logger L = LogManager.getLogger();

	private long last_file_length = 0;
	private FileReader file_reader = null;
	private BufferedReader buffer = null;

	private void open_file_and_read_config() throws IOException {
		File file = new File("/tmp/impresora");

		// DEBUG
		if (CONF.debug())
			file = new File("C:\\impresora");
		// DEBUG

		long current_size = file.length();

		if (current_size < this.last_file_length) {
			L.info("El fichero se ha reducido. Forzamos la reapurtura del mismo.");
			if (this.file_reader != null) {
				try {
					this.file_reader.close();
				} catch (IOException e) {
				}
				this.file_reader = null;
			}
			if (this.buffer != null) {
				try {
					this.buffer.close();
				} catch (IOException e) {
				}
				this.buffer = null;
			}
		}

		this.last_file_length = current_size;

		if (this.file_reader == null || this.buffer == null) {
			try {
				L.info("Abriendo el fichero [{}] y posicionamos el cursor en la posicion [{}].", file.getAbsoluteFile(),
						file.length());
				this.file_reader = new FileReader(file);
				buffer = new BufferedReader(this.file_reader);
				if (!CONF.debug())
					buffer.skip(file.length());
			} catch (IOException e) {
				L.error("Ocurrio una excepcion al abrir el fichero.");
				L.catching(e);
				try {
					if (this.file_reader != null)
						this.file_reader.close();
					if (this.buffer != null)
						this.buffer.close();
					this.file_reader = null;
					this.buffer = null;
				} catch (IOException e1) {
				}
				L.error("Ocurrio una excepcion al abrir el fichero.");
				L.catching(e);
				throw new IOException("Error al abrir el fichero de log", e);
			}
		}
	}

	public void operate() throws IOException {

		this.open_file_and_read_config();

		MongoCollection<Document> pedidos_collection = MongoProvider.getCollection("proyman", "pedidos");
		MongoCollection<Document> descartes_collection = MongoProvider.getCollection("proyman", "descartes");

		String line = null;
		while ((line = buffer.readLine()) != null) {

			ImpresoraLine l = new ImpresoraLine(line);

			boolean inserted = false;

			while (!inserted) {
				try {
					if (l.tipo == TipoLinea.DISCARD) {
						descartes_collection.insertOne(l.getDocumentForDiscard());
					} else {
						pedidos_collection.updateOne(l.getDocumentFilter(), l.getDocumentForUpdate(),
								new UpdateOptions().upsert(true));
					}
					inserted = true;
				} catch (MongoException ex) {
					L.error("Ocurrio una excepci�n al conectarse con la base de datos");
					L.catching(ex);

					boolean connected = false;
					while (!connected) {
						try {
							L.error("Intentando reconectar a la base de datos");
							MongoProvider.close();
							pedidos_collection = MongoProvider.getCollection("proyman", "pedidos");
							descartes_collection = MongoProvider.getCollection("proyman", "descartes");
							connected = true;
						} catch (Exception ex1) {
							try {
								L.warn("Ocurrio una excepci�n al conectarse con la base de datos [{}]",
										ex1.getMessage());
								L.warn("Reintentamos en breve");
								Thread.sleep(10000);
							} catch (InterruptedException unused) {
							}

						}
					}
				}
			}
		}
	}
}
