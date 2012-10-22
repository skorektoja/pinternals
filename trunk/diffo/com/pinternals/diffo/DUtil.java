/**
 * 
 */
package com.pinternals.diffo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
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
	
//	private static BlockingQueue<Callable> q = new PriorityBlockingQueue<Callable>();
	public static ExecutorService ex = null;
	
	private static boolean locked = false;
	public static File cachedir = new File(System.getProperty("java.io.tmpdir") + File.separator + "diffo" + File.separator);
	public DUtil(int threads) {
		cachedir.mkdir();
		log.config(DUtil.format("DUtil04cache", cachedir.getPath()));
		ex = Executors.newFixedThreadPool(threads);
		log.config(DUtil.format("DUtil05th", threads));
	}
	public static void shutdown() {
		ex.shutdown();
		log.info(DUtil.format("HUtil02"));
	}
	
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
		assert c!=null : "Connection isn't opened";
		assert key!=null && s!=null && !s.isEmpty() : "given statement is bad: " + key + s;
		if (log.isLoggable(Level.FINEST)) {
			int i = objs==null ? 0 : objs.length;
			log.fine(DUtil.format("prepareStatement", key, i, s.length()<50?s:s.substring(0,50)+"..."));
		}
		PreparedStatement ps = c.prepareStatement(s, PreparedStatement.RETURN_GENERATED_KEYS);
		assert ps != null : "prepareStatement failed";
		setStatementParams(ps, objs);
		return ps;
	}

	static boolean lock() {
		lock.lock();
		locked = true;
		return locked;
	}
	static boolean unlock(Connection cn) throws SQLException {
		cn.commit();
		lock.unlock();
		locked = false;
		return locked;
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

	synchronized static public FutureTask<HTask> addHTask(HTask h)  {
		if (log.isLoggable(Level.FINE))
			log.fine(DUtil.format("HUtil03addHTask", h.name));
		h.ok = false;
		FutureTask<HTask> f = new FutureTask<HTask>(h);
//		int i=0;
//		while (i++<1) //ex.getPoolSize() < 10 && i++<10
//			try {
//				ex.awaitTermination(10, TimeUnit.MILLISECONDS);
//			} catch (InterruptedException ignore) {
//				break;
//			}
		ex.execute(f);
		return f;
	}

	synchronized static void addTask(Runnable f)  {
		if (log.isLoggable(Level.FINE))
			log.fine(DUtil.format("DUtil03addTask","unk"));
		ex.execute(f);
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

class HTask implements Callable<HTask> {
	private static Logger log = Logger.getLogger(HTask.class.getName());
	Object refObj = null;
//	String cache = "";
	HttpURLConnection hc;
	String name, method, post;
//	boolean saveCache = false, readCache = false;
	int rc, attempts=0;
	ByteArrayInputStream bis = null;
	boolean ok = false;
	
	public String toString() {
		String s = "HTask " + name + " rc=" + rc + " ok=" + ok;
		return s;
	}
	private File cachefl() {
		File f = new File(DUtil.cachedir.getAbsolutePath() + File.separator + name + ".html");
		return f;
	}
	HTask (String name, HttpURLConnection hc) {
		this.name = name;
		method = "GET";
		post = null;
		assert hc.getRequestMethod().equals(method) : "method mismatch";
		this.hc = hc;
	}
	HTask (String name, HttpURLConnection hc, String post) {
		this.name = name;
		method = "POST";
		assert hc.getRequestMethod().equals(method) : "method mismatch";
		this.post = post;
		this.hc = hc;
	}
	HTask reset(HttpURLConnection hc2) {
		assert hc2!=null;
		hc = hc2;
		attempts++;
		return this;
	}
	void connect() {
		assert hc != null : "Null HttpURLConnection for HTask";
		ok = false;
		if (log.isLoggable(Level.FINE))
			log.fine(DUtil.format("HTask04connect", name, hc.getURL().toExternalForm()));
		rc = -1;
		try {
			if ("POST".equals(method))
				DUtil.putPOST(hc, post);
			hc.connect();
			rc = hc.getResponseCode();
			ok = (rc == HttpURLConnection.HTTP_OK);
		} catch (IOException e) {
			ok = false;
			e.printStackTrace();
		}
	}
	public HTask call() {
		boolean uf = log.isLoggable(Level.FINEST);
		ByteArrayOutputStream a = null;
		File flg = uf ? cachefl() : null;
		PrintStream fos = null;
		if (log.isLoggable(Level.FINEST)) {
			try { 
				flg.createNewFile();
				fos = new PrintStream(flg);
				fos.println("Http task " + name + "/" + method);
				if ("POST".equals(method)) 
					fos.println(post);
			} catch (Exception ignore) {
			}
		}
		connect();
		a = new ByteArrayOutputStream(65536);
		if (ok) {
			if (log.isLoggable(Level.CONFIG))
				log.config(DUtil.format("HTask05call", name));
			try {
				int i = hc.getInputStream().read();
				while (i!=-1) {
					a.write(i);
					if (fos!=null) fos.write(i);
					i = hc.getInputStream().read();
				}
			} catch (IOException ex) {
				ok = false;
				log.throwing(HTask.class.getCanonicalName(), "call", ex);
			}
		} else {
			log.severe(DUtil.format("HTask06call", name, rc));
			try {
				int i = hc.getErrorStream().read();
				while (i!=-1) {
					a.write(i);
					if (fos!=null) fos.write(i);
					i = hc.getErrorStream().read();
				}
			} catch (IOException ex) {
				log.throwing(HTask.class.getCanonicalName(), "call", ex);
			}
		}
		if (fos!=null) fos.close();
		hc.disconnect();
		
		bis = new ByteArrayInputStream(a.toByteArray());
		return this;
	}
}
