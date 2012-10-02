/**
 * 
 */
package com.pinternals.diffo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Formatter;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author Илья Кузнецов
 * 
 */
public class DUtil {
	public static String C_ENC = "UTF-8";
	private static Logger log = Logger.getLogger(DUtil.class.getName());
	
	private static ResourceBundle RES_SQL = ResourceBundle.getBundle("com.pinternals.diffo.sql");
	public static TreeSet<String> sqlKeySet = new TreeSet<String>(RES_SQL.keySet());
	private static ResourceBundle RES_MSG = ResourceBundle.getBundle("com.pinternals.diffo.messages");
	private static final Lock lock = new ReentrantLock();
	private static boolean locked = false;
//	private static int lockmax = 1;
//	private static BlockingQueue<Integer> locks = new ArrayBlockingQueue<Integer>(100);

	static boolean assertion() {
		return true;
	}
	
	public static String getSql(String key) throws MissingResourceException {
		return RES_SQL.getString(key);
	}

	/**
	 * @param key имя строки в messages.properties
	 * @param args параметры для форматирования
	 * @return строка для пользователя или для логов
	 */
	public static String format(String key, Object... args) {
		return new Formatter().format(RES_MSG.getString(key), args).toString();
	}

	/** 
	 * Возвращает версию, на которую рассчитана программа
	 * @return номер версии
	 */
	public static String getDbJarVersion() {
		return getSql("db_version");
	}
	/** 
	 * 
	 * @param app 
	 * @param key
	 * @param objs
	 */
	public static PreparedStatement setStatementParams(PreparedStatement ps,
			Object... objs) throws SQLException {
		int i = 1;
		String lg = ""; 
		if (objs != null) {
			lg = "setStatementParams("; 
			for (Object o : objs) {
				// be sure of proper order of type checking
				if (o==null)
					ps.setObject(i++, null);
				else if (o.getClass() == Long.class)
					ps.setLong(i++, (Long) o);
				else if (o.getClass() == Integer.class)
					ps.setInt(i++, (Integer) o);
				else if (o.getClass() == String.class)
					ps.setString(i++, (String) o);
				else if (o.getClass() == byte[].class)
					ps.setBytes(i++, (byte[]) o);
				else if (o.getClass() == Character.class)
					ps.setString(i++, String.valueOf(o));
				else if (o.getClass() == java.util.Date.class)
					ps.setLong(i++, ((java.util.Date)o).getTime() );
				else if (o.getClass() == Boolean.class)
					throw new RuntimeException(
						"SQLite doesn't support Boolean type. Isprav' kod, mudil^Utchelovetche!");
				else {
					String err = "setStatementParams was called for type " + o.getClass().getCanonicalName();
					assert false : err;
					throw new RuntimeException(err);
				}
				lg += o + ",";
			}
			lg += ")";
			if (log.isLoggable(Level.FINER)) log.finer(lg);
		} else 
			log.severe("setStatementParams with empty list");
		return ps;
	}
	public static PreparedStatement prepareStatementDynamic(Connection c, String sql) 
		throws SQLException {
		PreparedStatement ps = c.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
		return ps;
	}
	public static PreparedStatement prepareStatement(Connection c, String key, Object... objs)
			throws SQLException {
		log.entering(DUtil.class.getName(), "prepareStatement");
		String s = getSql(key);
		assert c!=null && key!=null && s!=null && !s.isEmpty();
		if (log.isLoggable(Level.FINEST)) {
			int i = objs==null ? 0 : objs.length;
			log.fine(DUtil.format("prepareStatement", key, i, s.length()<50?s:s.substring(0,50)+"..."));
		}
		PreparedStatement ps = c.prepareStatement(s, PreparedStatement.RETURN_GENERATED_KEYS);
		assert ps != null : "prepareStatement failed";
		setStatementParams(ps, objs);
		return ps;
	}

	static void lock() {
		lock.lock();
		locked = true;
		return;
	}
	static void unlock(Connection cn) throws SQLException {
		cn.commit();
		lock.unlock();
		locked = false;
		return;
	}

	public static int executeUpdate(PreparedStatement ps) throws SQLException {
		assert locked : "Database isn't locked";
        int i = ps.executeUpdate();
        return i;
	}
	public static int executeUpdate(PreparedStatement ps, long[] keys, int q) throws SQLException {
		assert locked : "Database isn't locked";
        int i = ps.executeUpdate();
        for (int j=1;j<=q;j++)
        	keys[j] = ps.getGeneratedKeys().getLong(j);
        return i;
	}
	public static int[] executeBatch(PreparedStatement ps) throws SQLException {
		assert locked : "Database isn't locked";
		int[] a = ps.executeBatch();
        return a;
	}
	// -------------------------------------------- работа с http
	public static HttpURLConnection getHttpConnection(Proxy prx, URL u, int millis) throws IOException {
		HttpURLConnection h = null;
		h = (prx != null) ? (HttpURLConnection) u.openConnection(prx)
				: (HttpURLConnection) u.openConnection();
		h.setDoOutput(true);
		h.setUseCaches(false);
		h.setDoInput(true);
		h.setAllowUserInteraction(false);
		h.setRequestProperty("Accept-Charset", C_ENC);
		h.setConnectTimeout(millis);
		return h;
	}	
	public static void putPOST(HttpURLConnection h, String query) throws IOException {
		OutputStream output = null;
        output = h.getOutputStream();
        output.write(query.getBytes(C_ENC));
	}
	public static byte[] readHttpConnection(HttpURLConnection h) throws IOException {
		ArrayList<Byte> a = new ArrayList<Byte>(100);
		InputStream is = h.getInputStream();
		int i = is.read();
		while (i!=-1) {
			a.add((byte)i);
			i=is.read();
		}
		byte b[] = new byte[a.size()];
		i=0;
		for (Byte y: a) {
			b[i++] = y;
		}
		return b;
	}
}

/* SimpleQuery handler */
class SimpleQueryHandler extends DefaultHandler {
	boolean td=false, h3=false, table=false, tr=false, th=false, he=false;
	Attributes aatts=null;
	String amount=null;
	int ia=0;
	String s = "";
	ArrayList<String> headers = null, row = null;
	ArrayList<ArrayList<String>> rows = new  ArrayList<ArrayList<String>>(10); 
	
	public void startElement(String uri, String name, String qName,	Attributes atts) {
		h3 = h3 || name.equalsIgnoreCase("h3");
		table = table || amount!=null && name.equalsIgnoreCase("table");
		tr = tr || table && name.equalsIgnoreCase("tr");
		if (name.equalsIgnoreCase("td")) {
			td = td || tr;
			aatts = null;
		}
		if (tr && td && name.equalsIgnoreCase("a")) {
			aatts = atts;
		}
		if (table && name.equalsIgnoreCase("th")) {
			th = true;
			he = true;
		}
		if (table && name.equalsIgnoreCase("tr")) {
			row = new ArrayList<String>(10);
		}
	}
	public void characters (char ch[], int b, int l) {s = new String(ch, b, l);}
	public void ignorableWhitespace(char ch[], int start, int length) {}
	public void endElement(String uri, String name, String qName) {
		if (h3 && name.equalsIgnoreCase("h3") && s.indexOf("Amount")!=-1) {
			amount = s.trim();
			ia = Integer.parseInt(amount.split("Amount of objects:")[1].trim());
			h3 = false;
		}
		if (table && tr && th && name.equalsIgnoreCase("th")) {
			row.add(s.trim());
			th = false;
		}
		if (table && tr && td && name.equalsIgnoreCase("td")) {
			if (aatts!=null) s=aatts.getValue("href");
			row.add(s.trim());
			td = false;
		}
		if (table && tr && name.equalsIgnoreCase("tr")) {
			if (he) 
				headers = row;
			else
				rows.add(row);
			row = new ArrayList<String>(10);
			he = false;
			tr = false;
		}
		table = table && !name.equalsIgnoreCase("table");
	}
	boolean test() {return ia==rows.size();}
}
/* SimpleQuery: entities and attributes */
class SQEntityAttr extends DefaultHandler {
	String sname;
	boolean st=false, opt=false;
	String[][] matrix = new String[300][2];	// up to 300 items.
	String optval, opttxt;
	private int i=0;
	public int size=-1;
	public SQEntityAttr(String name) {
		super();
		this.sname=name;
	}
	public void ignorableWhitespace(char ch[], int start, int length) {}
	public void startElement(String uri, String name, String qName,	Attributes atts) {
		String an = atts==null ? null : atts.getValue("name");
		st = st || (name.equals("select") && sname.equals(an) );
		if (st && name.equals("option")) {
			opt = true;
			optval = atts==null ? null : atts.getValue("value");
			opttxt = null;
		} else 
			opt = false;
	}
	public void characters (char ch[], int b, int l) {
		opttxt = st && opt ? new String(ch,b,l).trim() : null;
	}
	public void endElement(String uri, String name, String qName) {
		if (st && opt && "option".equalsIgnoreCase(name)) {
			opt = false;
			matrix[i][0] = optval;
			matrix[i][1] = opttxt;
			i++;
		}
		if (st && "select".equalsIgnoreCase(name)) {
			size = i;
			st = false;
		}
	}
}

