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
import java.util.Formatter;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

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
//	public static ResourceBundle logBundle = ResourceBundle.getBundle("logging.properties");
//	public static String logBundle = "com.pinternals.diffo.logging";
	
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
		if (objs != null)
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
					ps.setObject(i++, o);
				else if (o.getClass() == Boolean.class)
					throw new RuntimeException(
						"SQLite doesn't support Boolean type. Isprav' kod, mudil^Utchelovetche!");
				else {
					String err = "setStatementParams was called for type " + o.getClass().getCanonicalName();
					assert false : err;
					throw new RuntimeException(err);
				}
			}
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
	public static int executeUpdate(PreparedStatement ps, boolean commit) throws SQLException {
		lock.lock();
		try {
            int i = ps.executeUpdate();
            if (commit) ps.getConnection().commit();
            return i;
        } finally {
            lock.unlock();
        }
	}
	public static int executeUpdate(PreparedStatement ps, boolean commit, long[] keys, int q) throws SQLException {
		lock.lock();
		try {
            int i = ps.executeUpdate();
            if (commit) ps.getConnection().commit();
            for (int j=1;j<=q;j++)
            	keys[j] = ps.getGeneratedKeys().getLong(j);
            return i;
        } finally {
            lock.unlock();
        }
	}
	
	
	public static int[] executeBatch(PreparedStatement ps, boolean commit) throws SQLException {
		lock.lock();
		try {
			int[] a = ps.executeBatch();
			if (commit) ps.getConnection().commit();
            return a;
        } finally {
            lock.unlock();
        }
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
