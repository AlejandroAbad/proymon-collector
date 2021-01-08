package es.hefame.proymon.pedidos;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;

public class ImpresoraLine {

	private static Logger L = LogManager.getLogger();
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");

	static {
		DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	public enum TipoLinea {
		CHEQUEO_OK, PEDIDO_OK, INCIDENCIA, DISCARD
	}

	public enum MotivoDescarte {
		BADCRC, CLIENTE_PRUEBA, FORMATO, ABONO
	}

	public String origen;
	public String clisap;
	public String cliconc;
	public String fecha;
	public String hora;
	public String lineas;
	public String faltas;
	public String tipoped;
	public String crc;
	public String tipolinea;
	public String proceso;
	public String descripcion;
	public String segundos;
	public String fechaproc;
	public String horaproc;
	public String servidor;
	public String almacen;

	public String original;
	public TipoLinea tipo;
	public MotivoDescarte motivoDescarte;

	public ImpresoraLine(String original) {

		this.original = original;

		String[] chunks = original.split("\\|");

		if (chunks.length < 11) {
			tipo = TipoLinea.DISCARD;
			motivoDescarte = MotivoDescarte.FORMATO;
			return;
		}

		int i = 0;
		if (chunks.length > i)
			origen = chunks[i++].trim();
		if (chunks.length > i)
			clisap = chunks[i++].trim();
		if (chunks.length > i)
			cliconc = chunks[i++].trim();
		if (chunks.length > i)
			fecha = chunks[i++].trim();
		if (chunks.length > i)
			hora = chunks[i++].trim();
		if (chunks.length > i)
			lineas = chunks[i++].trim();
		if (chunks.length > i)
			tipoped = chunks[i++].trim();
		if (chunks.length > i)
			crc = chunks[i++].trim();
		if (chunks.length > i)
			tipolinea = chunks[i++].trim();
		if (chunks.length > i)
			proceso = chunks[i++].trim();
		if (chunks.length > i)
			descripcion = chunks[i++].trim();
		if (chunks.length > i)
			segundos = chunks[i++].trim();
		if (chunks.length > i)
			fechaproc = chunks[i++].trim();
		if (chunks.length > i)
			horaproc = chunks[i++].trim();
		if (chunks.length > i)
			servidor = chunks[i++].trim();
		if (chunks.length > i)
			almacen = chunks[i++].trim();
		if (chunks.length > i)
			faltas = chunks[i++].trim();

		if (crc.length() != 8) {
			tipo = TipoLinea.DISCARD;
			motivoDescarte = MotivoDescarte.BADCRC;
			return;
		}

		if (clisap.equals("0010108000")) {
			tipo = TipoLinea.DISCARD;
			motivoDescarte = MotivoDescarte.CLIENTE_PRUEBA;
			return;
		}

		if (tipolinea.equals("-SolDevol-")) {
			tipo = TipoLinea.DISCARD;
			motivoDescarte = MotivoDescarte.ABONO;
			return;
		}

		if (tipolinea.equals("-Chequeo--") && descripcion.length() == 0) {
			tipo = TipoLinea.CHEQUEO_OK;
			return;
		}

		try {
			long numPed = Long.parseLong(tipolinea);
			if (numPed != 1) {
				tipo = TipoLinea.PEDIDO_OK;
			} else {
				if (!origen.equals("10025")) {
					tipo = TipoLinea.PEDIDO_OK;
				} else {
					descripcion = "Pedido 1 con origen 10025 se rechaza";
					tipo = TipoLinea.INCIDENCIA;
				}
			}
		} catch (NumberFormatException nfe) {
			tipo = TipoLinea.INCIDENCIA;
		}

	}

	public Document getDocumentFilter() {
		return new Document("_id", crc);
	}

	public Document getDocumentForDiscard() {
		Document discarded = new Document();
		discarded.put("_id", new ObjectId());
		discarded.put("motivo", motivoDescarte.name());
		discarded.put("original", original);
		if (crc != null && crc.length() > 0)
			discarded.put("crc", crc);
		return discarded;
	}

	public Document getDocumentForUpdate() {

		Document setOnInsert = new Document();
		Document set = new Document();
		Document push = new Document();

		set.put("almacen", almacen);
		set.put("tipoped", tipoped);
		setOnInsert.put("origen", origen);
		setOnInsert.put("clisap", clisap);
		setOnInsert.put("cliconc", cliconc);
		setOnInsert.put("fecha", intOrNull(fecha));
		setOnInsert.put("hora", intOrNull(hora));
		setOnInsert.put("lineas", intOrNull(lineas));
		setOnInsert.put("timestamp", toDateUTC(fecha, hora));

		if (tipo == TipoLinea.CHEQUEO_OK) {

			Document checkeo = new Document();
			// checkeo.put("fecha", intOrNull(fechaproc));
			// checkeo.put("hora", intOrNull(horaproc));
			checkeo.put("servidor", servidor);
			checkeo.put("timestamp", toDateUTC(fechaproc, horaproc));
			checkeo.put("tiempoEjecucion", doubleOrNull(segundos));
			checkeo.put("faltas", intOrNull(faltas));
			set.put("chequeo", checkeo);
		} else {
			setOnInsert.put("checkeo", null);
		}

		if (tipo == TipoLinea.PEDIDO_OK) {
			Document pedido = new Document();
			// pedido.put("fecha", intOrNull(fechaproc));
			// pedido.put("hora", intOrNull(horaproc));
			pedido.put("servidor", servidor);
			pedido.put("pedido", tipolinea);
			pedido.put("timestamp", toDateUTC(fechaproc, horaproc));
			pedido.put("tiempoEjecucion", doubleOrNull(segundos));
			set.put("pedido", pedido);
			set.put("ok", true);
		} else {
			setOnInsert.put("pedido", null);
			setOnInsert.put("ok", false);
		}

		if (tipo == TipoLinea.INCIDENCIA) {
			if (descripcion.equals("DUPLIC.PROYMAN")) {
				setOnInsert.put("incidencia", false);
			} else {
				set.put("incidencia", true);
			}
		} else {
			setOnInsert.put("incidencia", false);
		}

		Document evento = new Document();
		// evento.put("fecha", intOrNull(fechaproc));
		// evento.put("hora", intOrNull(horaproc));
		if (descripcion.length() > 0)
			evento.put("descripcion", descripcion);
		evento.put("tipo", tipo.name());
		evento.put("original", original);
		evento.put("timestamp", toDateUTC(fechaproc, horaproc));
		push.put("eventos", evento);

		Document d = new Document();
		d.put("$setOnInsert", setOnInsert);
		d.put("$set", set);
		d.put("$push", push);
		return d;
	}

	private Integer intOrNull(String toInt) {
		if (toInt == null)
			return null;
		try {
			return Integer.parseInt(toInt);
		} catch (NumberFormatException e) {
			L.warn("No se puede convertir [{}] a Integer", toInt);
			return null;
		}
	}

	private Double doubleOrNull(String toDouble) {
		if (toDouble == null)
			return null;
		try {
			return Double.parseDouble(toDouble);
		} catch (NumberFormatException e) {
			L.warn("No se puede convertir [{}] a Double", toDouble);
			return null;
		}
	}

	public Long toDateUTC(String fecha, String hora) {
		if (fecha == null || hora == null)
			return null;
		try {
			return DATE_FORMAT.parse(fecha + hora).getTime();
		} catch (ParseException e) {
			L.warn("No se puede convertir [{}], [{}] a Date", fecha, hora);
			return null;
		}
	}

}