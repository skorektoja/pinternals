package com.pinternals.diffo;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.xml.sax.SAXException;

import com.pinternals.diffo.PiEntity.ResultAttribute;
import com.pinternals.diffo.PiObject.Kind;
import com.pinternals.diffo.api.IDiffo;

/**
 * @author Илья Кузнецов Обращается к БД, ведёт сессию и хранит список хостов,
 *         должен быть одним на сеанс работы. Также отдаёт назад результаты сравнений.
 */
public class Diffo implements IDiffo, Cloneable {
	private static Logger log = Logger.getLogger(Diffo.class.getName());
	public static String version = "0.1.1";
	private List<PiHost> pihosts = new ArrayList<PiHost>(10);
	private Connection conn = null;
	private File dbfile = null;
	
	public Long session_id = -1L;
	public Proxy proxy;
	private int threadsindexing = 5; 
	private List<Thread> incoming = new LinkedList<Thread>(), 
			workers = new LinkedList<Thread>();

	/**
	 * @param dbname путь к файлу БД
	 * @param prx    http-proxy (optional)
	 */
	public Diffo(String dbname, Proxy prx, int tx) {
		proxy = prx;
		dbfile = new File(dbname);
		threadsindexing = tx;
		log.entering(Diffo.class.getName(), "Diffo");
	}

	/** *****************************************************
	 * Возвращает версию из БД
	 * 
	 * @return номер версии
	 * @throws SQLException
	 */
	public String getDbFileVersion() throws SQLException {
		assert conn!=null;
		ResultSet r = DUtil.prepareStatement(conn, "sql_config_getversion").executeQuery();
		String dbVersion = r.next() ? r.getString(1) : null;
		log.info(DUtil.format("getDbFileVersion", dbVersion));
		return dbVersion;
	}

	/** 
	 * Use only when session is opened. 
	 * @param key
	 * @param objs
	 * @return sql statement
	 * @throws SQLException
	 */
	protected PreparedStatement prepareStatement(String key, Object... objs)
			throws SQLException {
		if (session_id==-1L || session_id==0) {
			assert false : "session must be opened";
			throw new RuntimeException("Session is not opened when attempt to access to SQL");
		}
		return DUtil.prepareStatement(conn, key, objs);
	}

	/**
	 * Проверяет БД на непротиворечивость, установлено в ассертах или изредка напрямую
	 * @throws SQLException
	 */
	public boolean validatedb() throws SQLException {
		ResultSet rs = DUtil.prepareStatement(conn, "sql_check01").executeQuery();
		assert rs != null;
		String s = "";
		boolean b = false;
		while (rs.next()) {
			b = true;
			s = s + rs.getLong(1) + "\t" + rs.getString(2) + "\n";
		}
		assert !b : "DB consistency error. Objects/versions logic failed!\n" + s;
		if (b) log.log(Level.SEVERE, DUtil.format("validatedb_error", s));
		
		s="";
		rs = DUtil.prepareStatement(conn, "sql_icheck").executeQuery();
		if (rs.next()) {
			s=rs.getString(1);
			log.config("DB integrity check: " + s);
			b = b && s.equals("ok");
		}
		for (PiHost p: pihosts) {
			rs = DUtil.prepareStatement(p.hostdb, "sql_icheck").executeQuery();
			if (rs.next()) {
				s=rs.getString(1);
				log.config("DB integrity check: " + s);
				b = b && s.equals("ok");
			}
		}
		return !b;
	}

	/**
	 * Создаёт БД по указанному соединению
	 * 
	 * @param cn
	 *            соединение (обычно на диске или в памяти)
	 * @return true if OK, false if ERROR (log output)
	 */
	private static boolean createdb(Connection cn) {
		assert cn != null;
		try {
			for (String k : DUtil.sqlKeySet)
				if (k.startsWith("sql_init"))
					DUtil.prepareStatement(cn, k).executeUpdate();
			for (String k : DUtil.sqlKeySet)
				if (k.startsWith("sql_0pendb_"))
					DUtil.prepareStatement(cn, k).executeUpdate();
			if (!cn.getAutoCommit())
				cn.commit();
		} catch (SQLException ex) {
			log.log(Level.SEVERE, "CreateDB failed", ex);
			return false;
		}
		return true;
	}
	public boolean createdb() throws SQLException, ClassNotFoundException {
		boolean a, b;
		assert conn!=null && !conn.isClosed() : "Database must be opened before create DB";
		a = session_id!=-1L;
		if (a) finish_session();
		closedb();		// close prev connection
		conn = null;	// release it completely 
		b = opendb();
		b = b && createdb(conn) && commit();
		if (a) b = b && start_session();
		if (b) {
			int i = DUtil.prepareStatement(conn, "sql_config_putversion", 
						DUtil.getSql("db_version"), session_id).executeUpdate();
			commit();
		}
		return b;
	}

	public boolean commit() throws SQLException {
		conn.commit();
		return true; // for using in conditions
	}
	public boolean rollback() throws SQLException {
		conn.rollback();
		return true; // for using in conditions
	}
	void migrateMainDB(String newDbName) throws SQLException {
		assert conn!=null && !conn.isClosed() : "current main db must be opened before migration";
//		assert !newDbName.contains("./\\") : "dot or slashes in temp db aren't supported for attach yet";
		Connection newcon = DriverManager.getConnection("jdbc:sqlite:" + newDbName);
		newcon.setAutoCommit(false);
		createdb(newcon);
		newcon.close();

		//IMPORTANT!!! главная база должна быть в автокоммите
		//see http://stackoverflow.com/a/9119346/521359
		//for DDL statement only it's required
		conn.setAutoCommit(true);
		// здесь не передать newDbName с точкой, как-то экранировать что-ли надо
		String s = "ATTACH DATABASE '" + newDbName + "' AS migr"; 
		PreparedStatement ps = DUtil.prepareStatementDynamic(conn, s);
		ps.executeUpdate();
		// все DML-операции уже могут быть коммичены явно
		conn.setAutoCommit(false);
		//TODO: добавить проверку валидности migrsql_tables и реального
		//lst задаёт порядок (зависимости), но никто не гарантирует полного списка таблиц 
		String[] lst = DUtil.getSql("migrsql_tables").split(",");
		for (String t: lst) {
			s = "DELETE FROM migr." + t;
			ps = DUtil.prepareStatementDynamic(conn, s);
			ps.executeUpdate();
			conn.commit();
			s = "INSERT INTO migr." + t + " SELECT * FROM " + t;
			ps = DUtil.prepareStatementDynamic(conn, s);
			ps.executeUpdate();
			conn.commit();
		}
	}

	/**
	 * Создаёт БД в памяти, применяет к ней все выражения
	 * 
	 * @return true if DBMS is well
	 */
	public static boolean simulatedb() throws ClassNotFoundException {
		String t = "";
		Connection cn = null;
		boolean ok = true;
		Class.forName("org.sqlite.JDBC");
		try {
			cn = DriverManager.getConnection("jdbc:sqlite::memory:");
			// cn = DriverManager.getConnection("jdbc:sqlite:test.db");
			cn.setAutoCommit(true);
		} catch (SQLException ex) {
			ok = false;
		} finally {
			ok = createdb(cn);
		}
		if (ok) {
			for (String k : DUtil.sqlKeySet)
				if (k.startsWith("sql_") && !k.startsWith("sql_initdb")
						&& !k.startsWith("sql_0pendb_")) {
					try {
						t += "\n[" + k + "]\n" + DUtil.getSql(k);
						DUtil.prepareStatement(cn, k);
					} catch (SQLException ex) {
						ok = false;
						log.severe(DUtil.format("simulatedb_prepare", k, DUtil
								.getSql(k), ex.getMessage()));
					}
				}
			try {
				ok = ok && !cn.isClosed();
				cn.close();
				ok = ok && cn.isClosed();
			} catch (SQLException ex) {
				log.severe(DUtil.format("simulatedb_close", ex.getMessage()));
				ok = false;
			}
		}
		if (log.isLoggable(Level.FINER))
			log.finer(t);
		log.log(ok ? Level.INFO : Level.SEVERE, DUtil.format("simulatedb", ok));
		return ok;
	}

	/**
	 * Open diffo.db
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public boolean opendb() throws ClassNotFoundException, SQLException {
		Class.forName("org.sqlite.JDBC");
		if (conn != null) {
			conn.close();
			conn = null;
		}
		assert dbfile != null : "dbfile must be initiated in constructor";

		conn = DriverManager.getConnection("jdbc:sqlite:" + dbfile.getAbsolutePath());
		conn.setAutoCommit(false);
		boolean ok = true;
		for (String s : DUtil.sqlKeySet)
			if (s.startsWith("sql_0pendb_")) DUtil.prepareStatement(conn, s).execute();
		log.log(ok ? Level.INFO : Level.SEVERE, 
				DUtil.format("opendb", dbfile.getAbsolutePath(), dbfile.length(), ok));
		return ok;
	}

	/**
	 * Если число распределённых страниц больше 0, считает базу существующей
	 * 
	 * @return
	 * @throws SQLException
	 */
	public boolean isDbExist() throws SQLException {
		assert conn!=null && !conn.isClosed();
		ResultSet rs = DUtil.prepareStatement(conn, "sql_page_count").executeQuery();
		return rs.next() ? rs.getInt(1) > 0 : false;
	}

	public boolean start_session() throws SQLException {
		assert conn != null && !conn.isClosed() : "database must be opened";
		PreparedStatement ps = DUtil.prepareStatement(conn, "sql_startsession");
		boolean b = ps.executeUpdate() == 1	&& commit();
		if (b)
			session_id = ps.getGeneratedKeys().getLong(1);
		else
			session_id = -1L;
		return b;
	}

	public void finish_session() throws SQLException {
		assert session_id != -1L;
		int i = prepareStatement("sql_finishsession", new Long(session_id))
				.executeUpdate();
		assert i == 1;
		commit();
		log.info(DUtil.format("finish_session", session_id));
		session_id = -1L;
		shutdown();
	}

	public void closedb() throws SQLException {
		conn.commit();
		log.info(DUtil.format("closedb"));
		if (session_id != -1L)
			finish_session();
		for (PiHost p: this.pihosts) p.close();
		conn.close();
	}

	// ----------------------------------------------------------------------------------------------
	public PiHost addPiHost(String sid, String url)
			throws SQLException, MalformedURLException, IOException {
		assert conn!=null;
		log.entering(Diffo.class.getName(), "addPiHost");
		log.fine(DUtil.format("addPiHost_call", sid, url));
		long l;
		PreparedStatement ps = prepareStatement("sql_host_get", sid, url);
		ResultSet rs = ps.executeQuery();
		if (rs.next())
			l = rs.getLong(1);
		else {
			ps = prepareStatement("sql_host_put", sid, url, session_id);
			ps.executeUpdate();
			l = ps.getGeneratedKeys().getLong(1);
			commit();
		}

		String hostdbn = dbfile.getName();
		if (hostdbn.indexOf('.') != -1) {
			String[] s = hostdbn.split("[.]");
			assert s.length == 2;
			hostdbn = s[0] + "." + sid + "." + l + "." + s[1];
			if (dbfile.getParent() != null)
				hostdbn = dbfile.getParent() + File.separatorChar + hostdbn;
		} else
			hostdbn = dbfile.getAbsolutePath() + "." + sid + "." + l + ".db";
		File hostdb = new File(hostdbn);
		log.config(DUtil.format("addPiHost_hostfile", sid, hostdb.getAbsoluteFile(), hostdb.length()));
		
		Connection hostcon = null;
		hostcon = DriverManager.getConnection("jdbc:sqlite:" + hostdb.getAbsolutePath());
		hostcon.setAutoCommit(false);
		boolean b = false;
		rs = DUtil.prepareStatement(hostcon, "sql_page_count").executeQuery();
		b = rs.next() && rs.getLong(1)!=0;
		if (!b) { 
			PiHost.createHostDB(hostcon);
		}
		PiHost p = new PiHost(this, sid, url, l, hostcon);
		// читаем конфиг
		rs = prepareStatement("sql_config_gethost", p.host_id).executeQuery();
		while (rs.next()) {
			log.config(DUtil.format("addPiHost_config", rs.getString(1), rs.getString(2)));
		}
//		String imesapcom = getConfig(p, "ime_head_sap"), imecust = getConfig(p,
//				"ime_head_customer");
//		if (imesapcom == null) {
//			p.ime_sapcom = false;
//			updateConfig(p, "ime_head_sap", p.ime_sapcom ? "true" : "false");
//		} else
//			p.ime_sapcom = Boolean.parseBoolean(imesapcom);
//		if (imecust == null) {
//			p.ime_customer = true;
//			updateConfig(p, "ime_head_customer", p.ime_customer ? "true"
//					: "false");
//		} else
//			p.ime_customer = Boolean.parseBoolean(imecust);
//		// verbose(Verbose.INFO,"host " + sid + " " + url + " has id " + l);
		pihosts.add(p);
		return p;
	}

	/*
	 * private String getConfig(PiHost p, String key) { ResultSet rs = null;
	 * assert p != null : "host must be present"; try { rs =
	 * applyQuery(prepareStatement("sql_config_getone", getSql(key),
	 * p.host_id)); return rs == null ? null : rs.getString(1); } catch
	 * (SQLException e) { return null; } }
	 * 
	 * private boolean updateConfig(PiHost p, String key, String value) throws
	 * SQLException { assert p != null : "host is null"; //
	 * sql_config_put=INSERT INTO config (property,value,session_id,host_id) //
	 * VALUES (?1,?2,?3,?4); // sql_config_upd=UPDATE config SET value=?2,
	 * session_id=?3 WHERE // property=?1 AND host_id=?4; PreparedStatement upd
	 * = prepareStatement("sql_config_upd", getSql(key), value, session_id,
	 * p.host_id); int i = upd.executeUpdate(); if (i == 0) { upd =
	 * prepareStatement("sql_config_put", getSql(key), value, session_id,
	 * p.host_id); i = upd.executeUpdate(); } assert i == 1 :
	 * "configuration key " + getSql(key) + " for host_id " + p.host_id +
	 * " not updated"; return i == 1; }
	 * 
	 */ 
	public void refreshMeta(PiHost p) throws SQLException, IOException, SAXException, InterruptedException, ExecutionException {
		boolean force = false;

		List<PiEntity> db, online, mrgd = new LinkedList<PiEntity>();
		db = getMetaDB(p, Side.Repository);
		if ((db.isEmpty() || force) && p.isSideAvailable(Side.Repository)) {
			online = p.getMetaOnline(Side.Repository);
			mergeMeta(mrgd, db, online);
			commit();
		} else
			mrgd.addAll(db);
		
		db = getMetaDB(p, Side.Directory);
		if ((db.isEmpty() || force) && p.isSideAvailable(Side.Directory)) {
			online = p.getMetaOnline(Side.Directory);
			mergeMeta(mrgd, db, online);
			commit();
		} else
			mrgd.addAll(db);

//		db = getMetaDB(p, Side.SLD);
//		if ((db.isEmpty() || force) && p.isSideAvailable(Side.SLD)) {
//			online = p.getMetaOnline(Side.SLD);
////			mergeMeta(db, online);
//			commit();
//		}
		assert mrgd.size()>0;
		p.entities.clear();
		for (PiEntity e: mrgd)
			p.addEntity(e);
		assert p.entities.size()>0;
		assert p.entities.size()==mrgd.size();
	}

	public List<PiEntity> getMetaDB(PiHost p, Side side) throws SQLException {
		assert p!=null && side!=null;
		PreparedStatement psa = prepareStatement("sql_ra_getone")
			, eg = prepareStatement("sql_entities_getside", p.host_id, side.txt());
		ResultSet rs = eg.executeQuery(), rsa;
		List<PiEntity> db = new ArrayList<PiEntity>(0);
		while (rs.next()) {
			PiEntity x = new PiEntity(p,rs.getLong(1),side,rs.getString(2),rs.getString(3),rs.getInt(4));
			rsa = DUtil.setStatementParams(psa, x.entity_id).executeQuery();
			while (rsa.next())
				x.addAttr(rsa.getString(1), rsa.getString(2), rsa.getInt(3));
			db.add(x);
		}
		return db;
	}
	public void mergeMeta(List<PiEntity> mrgd, List<PiEntity> db, List<PiEntity> online) throws SQLException {
		PreparedStatement inse = prepareStatement("sql_entities_ins")
				, insa = prepareStatement("sql_ra_ins");
		int i, q, sdb = db.size(), sonl = online.size();
		for (PiEntity x: online) {
			PiHost p = x.host;
			q = db.indexOf(x);
			if (q == -1) {
				// Есть онлайн, нету в БД => добавляем
				log.fine("try to add entity " + x.side.txt() + "|" + x.intname);
				// INSERT INTO entity(side,internal,host_id,caption,seqno,session_id) VALUES (?1,?2,?3,?4,?5,?6);
				DUtil.setStatementParams(inse,x.side.txt(),x.intname,p.host_id,x.title,x.seqno,session_id);
				i = inse.executeUpdate();
				assert i==1;
				x.entity_id = inse.getGeneratedKeys().getLong(1);
				for (ResultAttribute a: x.attrs) {
					// добавляем и атрибуты
					// NSERT INTO ra(entity_id,raint,racaption,seqno) VALUES (?1,?2,?3,?4);
					DUtil.setStatementParams(insa,x.entity_id,a.internal,a.caption,a.seqno);
					insa.addBatch();
				}
				insa.executeBatch();
				mrgd.add(x);
			} else {
				assert x.attrs.size()==db.get(q).attrs.size() : "Attributes were changeed. System upgraded?";
				mrgd.add(x);
				db.remove(q);
			}
		}
		assert mrgd.size()>sdb || mrgd.size()>sonl : "Merged size too small";
	}


	
	public boolean putIndexRequestInQueue(final PiHost p, final PiEntity e) {
//		Thread w = new Thread(new Runnable(){
//			public void run() {
//				try {
//					if (e.side == Side.Repository)
//						handleIndexRepositoryObjectsVersions(p.askIndex(e), p, e);
//					else if (e.side == Side.Directory)
//						handleIndexDirectoryObjectsVersions(p.askIndex(e), p, e);
//					else
//						new RuntimeException("There is no handler for " + e.side + "|" + e.intname);
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			}
//		});
		return false; // incoming.add(w);
	}
	
	public boolean tickIndexRequestQueue(boolean loop) {
		boolean b = incoming.size()!=0 || workers.size()!=0;
		while (b) {
			if (incoming.size()>0 && workers.size()<threadsindexing) {
				Thread w = incoming.remove(0);
				w.start();
				workers.add(w);
			}
			for (Thread w: workers) if (!w.isAlive()) { 
				workers.remove(w);
				break;
			}
			b = incoming.size()!=0 || workers.size()!=0;
			b = b && loop;
		}
		return incoming.size()!=0 || workers.size()!=0;
	}
	

	public void askIndexRepository(PiHost p) {
		String[] arr={"namespdecl",
				"ifmtypedef",
				"XI_TRAFO",
				"AdapterMetaData",
				"ifmcontobj",
				"ifmtypeenh",
				"ifmextdef",
				"ifmextmes",
				"ifmfaultm",
				"FUNC_LIB",
				"FUNC_LIB_PROG",
				"ChannelTemplate",
				"MAP_ARCHIVE_PRG",
				"MAPPING",
				"AlertCategory",
				"TRAFO_JAR",
				"RepBProcess",
				"rfc",
				"idoc",
				"imsg",
				"iseg",
				"ityp",
				"ifmclsfn",
				"ifmmessage",
				"ifmoper",
				"processcomp",
				"process",
				"rfcmsg",
				"ifmmessif",
				"MAPPING_TEST",
				"DOCU",

				"arismodelext",
				"arisprofile",
				"ariscxnocc",
				"arisobjocc",
				"aristextocc",
		};
		for (String x: arr) {
			PiEntity e = p.getEntity(Side.Repository, x);
			if (e==null) 
				log.severe("Entity is null: Repository/" + x);
			else
//				p.getIndex(e);
				putIndexRequestInQueue(p, e);
		}
	}
	
	public void askIndexDirectory(PiHost p)  {
		String[] arr = {"Party",
				"Service",
				"Channel",
				"InboundBinding",
				"OutboundBinding",
				"RoutingRelation",
				"RoutingRule",
				"MappingRelation",
				"P2PBinding",
				"DirectoryView",
				"ValueMapping",
				"DOCU",};
		for (String x: arr) {
			PiEntity e = p.getEntity(Side.Directory, x);
			if (e==null) 
				log.severe("Entity is null: Directory/" + x);
			else
				putIndexRequestInQueue(p, e);
		}
	}


	private void handleIndexDirectoryObjectsVersions(List<PiObject> objs, PiHost p, PiEntity e)
			throws SQLException {
		assert objs!=null && p!=null && e!=null;
		PreparedStatement sel = prepareStatement("sql_objdir_report", e.entity_id)
			, del = prepareStatement("sql_objdir_del")
			, deactV = prepareStatement("sql_ver_deactv")
			, ins = prepareStatement("sql_objdir_ins")
			// INSERT INTO version (object_ref,version_id,session_id,is_active) VALUES (?1,?2,?3,?4)
			, insV = prepareStatement("sql_ver_ins") 

			;

		// Вставить гуиды, полученные онлайн, в tmp3
		fill_tmp4(objs, e);
		
		// делаем искалку. Так только для Directory! в Repository будет +SWCV и +SP
		HashMap<ByteBuffer, PiObject> hm = new HashMap<ByteBuffer, PiObject>(objs.size());
		for (PiObject o: objs) {
			hm.put(ByteBuffer.wrap(o.objectid), o);
		}

		ResultSet rs = sel.executeQuery();
		assert rs!=null;
		int i;
		long[] keys = new long[10];
		while (rs.next()) {
			String txt = rs.getString(1);
			byte[] oid = rs.getBytes(2), vid=rs.getBytes(3);
			long oref = rs.getLong(4);
			
			PiObject o = hm.get(ByteBuffer.wrap(oid));
			assert o!=null : "UNKNOWN OBJECT reference " + oref;
			assert UUtil.areEquals(o.versionid,vid);
			assert UUtil.areEquals(o.objectid,oid);

			/*
			if (txt.equals("CURRENT_LIVE") || txt.equals("CURRENT_DEAD")) 
				o.refDB = oref;
			if (txt.equals("NEWVER_LIVE")) {
				o.refDB = oref;
				// появилась новая версия объекта. 
//				zip = DUtil.readHttpConnection(p.establishGET(new URL(o.rawref), true));
				// Деактивируем старую версию, сессия не меняется
				DUtil.setStatementParams(deactV,oref);
				i = DUtil.executeUpdate(deactV, true);
				assert i==1 : "deactivate failed, rows updated:"+i;
				// добавляем новую активную версию 
				DUtil.setStatementParams(insV, o.refDB, o.versionid, session_id, 1);
				i = DUtil.executeUpdate(insV,true);
				assert i==1 : "insert version touched rows: " + i;
				p.addObject(o, session_id);
			} else if (txt.equals("NEWVER_DEAD")) {
				o.refDB = oref;
				// появилась новая версия объекта и она удалена.
				DUtil.setStatementParams(insV, o.refDB, o.versionid, session_id, 0);
				i = DUtil.executeUpdate(insV, true);
				assert i==1 : "insert version touched rows: " + i;
				DUtil.setStatementParams(del, o.refDB, session_id);
				i = DUtil.executeUpdate(del, true);
				assert i==1 : "update failed, rows:" + i;
			} else if (txt.equals("NEWOBJECT")) {
				// объект может быть удалённым, но о нём мы узнаём впервые.
//				zip = o.deleted? null: DUtil.readHttpConnection(p.establishGET(new URL(o.rawref), true));
				DUtil.setStatementParams(ins,p.host_id,session_id,o.objectid,o.e.entity_id,o.rawref,o.deleted?1:0);
				i = DUtil.executeUpdate(ins, true, keys, 1);
				assert i==1 : "insert object failed, rows touched:"+i;
				o.refDB = keys[1];
				// добавляем новую активную версию
				DUtil.setStatementParams(insV,o.refDB,o.versionid,session_id,o.deleted?0:1);
				i = DUtil.executeUpdate(insV, true);
				if (!o.deleted) p.addObject(o, session_id);
			}
			*/
		}
//		p.commitObject();
		i = 0;
//		for (PiObject o: objs) if (o.refDB<1){
//			i++;
//		}
		assert i==0 : "" + i + " objects are not handled; all amount is " + objs.size() ;
	}
	private void fill_tmp4(List<PiObject> a, PiEntity e) throws SQLException {
//		DUtil.executeUpdate(prepareStatement("sql_tmp4_del", e.entity_id), true);
//		PreparedStatement ins = prepareStatement("sql_tmp4_ins");
//		assert a!=null;
//		for (PiObject o: a) {
//			DUtil.setStatementParams(ins, e.entity_id, o.objectid, o.versionid, o.deleted?1:0);
//			ins.addBatch();
//		}
//		DUtil.executeBatch(ins, true);
	}
	private void fill_tmp8(List<PiObject> a, PiHost p, PiEntity e) throws SQLException {
/*		PreparedStatement
				del = prepareStatement("sql_tmp8_del", e.entity_id)
				, ins = prepareStatement("sql_tmp8_ins")
				, sel=prepareStatement("sql_tmp8_idx", e.entity_id);
		DUtil.executeUpdate(del,true);
		assert a!=null;
		int i=0;
		for (PiObject o: a) {
			Object []x = o.extrSwcvSp();
			// (entity_id,oid,vid,swcv,sp,del,host_id)
			DUtil.setStatementParams(ins,e.entity_id,o.objectid, o.versionid, x[0], x[1], o.deleted?1:0, p.host_id, i++);
			ins.addBatch();
		}
		DUtil.executeBatch(ins, true);
		ResultSet rs = sel.executeQuery();
		i = 0;
		while (rs.next()) {
			a.get(i).refSWCV = rs.getLong(1);
			assert a.get(i).refSWCV!=0;
			assert i==rs.getLong(2);
			i++;
		}
		rs.close();
		// Здесь должно остаться содержимое в tmp8 со ссылками на объекты
		 */
	}

	private void handleIndexRepositoryObjectsVersions(List<PiObject> objs, PiHost p, PiEntity e)
	throws SQLException {
		assert objs!=null && p!=null && e!=null;
		log.entering(Diffo.class.getCanonicalName(), "handleIndexRepositoryObjectsVersions");
		final PreparedStatement sel = prepareStatement("sql_objrep_report", e.entity_id)
			, del = prepareStatement("sql_objrep_del")
			, deactV = prepareStatement("sql_ver_deactv")
			, ins = prepareStatement("sql_objrep_ins")
			, insV = prepareStatement("sql_ver_ins")
			;
		boolean ignore_sap_deleted = true;	// true для игнорирования удалённых SAP-объектов
		boolean ignore_sap_alive = true;    // true для игнорирования живых SAP-объектов
		
		// Вставить гуиды, полученные онлайн, в tmp8 И УЗНАТЬ ССЫЛКИ REF
		fill_tmp8(objs, p, e);

		// делаем искалку
		HashMap<ByteBuffer,List<PiObject>> hm = new HashMap<ByteBuffer,List<PiObject>>(objs.size());
		for (PiObject o: objs) {
			ByteBuffer b = ByteBuffer.wrap(o.objectid);
			List<PiObject> t = hm.get(b);
			if (t==null) {
				t = new ArrayList<PiObject>(2);
				t.add(o);
				hm.put(b,t);
			} else
				t.add(o);
		}

		ResultSet rs = sel.executeQuery();
		assert rs!=null : "query 'sql_objrep_report' crashed to select";
		int i, sapdeleted=0, sapalive=0;
		SWCV swcv = null;

		while (rs.next()) {
			String txt = rs.getString(1);
			byte[] oid = rs.getBytes(2), vid=rs.getBytes(3);
			long swcref=rs.getLong(4), oref = rs.getLong(5);
			if (log.isLoggable(Level.FINE))
				log.fine(DUtil.format("sql_objrep_report", e.intname, txt, UUtil.getStringUUIDfromBytes(oid), UUtil.getStringUUIDfromBytes(vid), swcref, oref ));

			List<PiObject> ol = hm.get(ByteBuffer.wrap(oid));
			PiObject o = null;
			swcv = null;
			if (ol!=null) for (PiObject oi : ol) {
				if (oi.refSWCV==swcref && 
					UUtil.areEquals(oid, oi.objectid) &&
					UUtil.areEquals(vid, oi.versionid) ) {  
					o = oi;
					swcv = p.swcv.get(swcref);
					break;
				}
			}
			
/*			
			long keys[] = new long[10];
			if (txt.equals("CURRENT_LIVE") || txt.equals("CURRENT_DEAD")) 
				o.refDB = oref;
			else if (txt.equals("NEWOBJECT")) {
				if (log.isLoggable(Level.FINE))
					log.fine("is deleted:" + o.deleted);
				// объект может быть удалённым, но о нём мы узнаём впервые.
				if (o.deleted && swcv.is_sap() && ignore_sap_deleted) {
					// Не стоит записывать удалённые саповские объекты, они замусоривают базу
					sapdeleted++;
				} else if (!o.deleted && swcv.is_sap() && ignore_sap_alive) {
					// Не стоит записывать удалённые саповские объекты, они замусоривают базу
					sapalive++;
				} else {
//					zip = o.deleted? null: p.readHttpConnection(p.establishGET(new URL(o.rawref), true));
					if (log.isLoggable(Level.FINE))
						log.fine("before sql_objrep_ins for " + o.rawref );
					DUtil.setStatementParams(ins,p.host_id,session_id,o.refSWCV,o.objectid,o.e.entity_id,o.rawref,o.deleted?1:0);
					i = DUtil.executeUpdate(ins,true,keys,1);
					assert i==1: "insertion REP object failed, rows affected:"+i;
					o.refDB = keys[1];
					assert o.refDB!=0 : "bad object_ref:" + o.refDB;
					DUtil.setStatementParams(insV, o.refDB, o.versionid, session_id, o.deleted?0:1);
					i = DUtil.executeUpdate(insV, true);
					assert i==1: "insertion REP version failed, rows affected:"+i;
					if (!o.deleted) {
						if (log.isLoggable(Level.FINE))
							log.fine("try to add object " + o + " in session " + session_id);
						p.addObject(o, session_id);
					}
				}
			} else if (txt.equals("NEWVER_LIVE")) {
				o.refDB = oref;
				// появилась новая версия объекта. 
//				zip = DUtil.readHttpConnection(p.establishGET(new URL(o.rawref),true));
				// Деактивируем старую версию, сессия не меняется
				DUtil.setStatementParams(deactV,oref);
				i = DUtil.executeUpdate(deactV, true);
				assert i==1 : "deactivate failed, rows updated:"+i;
				// добавляем новую активную версию
				DUtil.setStatementParams(insV, o.refDB, o.versionid, session_id, 1);
				i = DUtil.executeUpdate(insV, true);
				assert i==1 : "insert version touched rows: " + i;
				p.addObject(o, session_id);
			} else if (txt.equals("NEWVER_DEAD")) {
				assert !swcv.is_sap();
				o.refDB = oref;
				// появилась новая версия объекта и она удалена.
				DUtil.setStatementParams(insV, o.refDB, o.versionid, session_id, 0, null);
				i = DUtil.executeUpdate(insV, true);
				assert i==1 : "insert version touched rows: " + i;
				DUtil.setStatementParams(del, o.refDB, session_id);
				i = DUtil.executeUpdate(del, true);
				assert i==1 : "update failed, rows:" + i;
			} else {
				i = 0/0;
			}
			*/
		}
		i = -sapdeleted - sapalive;
		for (PiObject o: objs) if (o.refDB<1){
			i++;
		}
		if (i!=0) {
			assert i==0: i+" objects are not handled; all amount is " + objs.size();
		}
//		p.commitObject();
	}

	public void askSld(PiHost p) {
		Side l = Side.SLD;
		PiEntity slds[] = {
				p.getEntity(l, "SAP_BusinessSystem"),
				p.getEntity(l, "SAP_BusinessSystemGroup"),
				p.getEntity(l, "SAP_BusinessSystemPath"),
		};
		for (PiEntity e: slds) {
			// TODO: not implemented yet
//			ArrayList<PiObject> objs = p.askIndex(e);
		}
	}	

	public void askCc(PiHost p) throws SQLException {
		ResultSet rs = prepareStatement("sql_cc_list", p.host_id)
				.executeQuery();
		if (rs != null)
			while (rs.next()) {
//				byte m[] = rs.getBytes(3);
//				URL u = new URL(rs.getString(2));
//				boolean is_deleted = rs.getLong(1) == 1;
//				boolean is_incpa = p.cpacache.isChannelInCPA(m);
//				boolean ok;
//				if (!is_deleted && is_incpa) {
//					RawRef r = new RawRef(u.getQuery());
//					ok = p.askCcStatus(m);
//					assert ok : "askCcStatus failed";
//				} else if (!is_deleted && !is_incpa) {
//					System.err.println("not in CPA cache but not deleted "
//							+ rs.getString(5));
//				}
			}
	}
	
	public List<DiffItem> list (PiHost p, PiEntity el) throws SQLException, IOException {
		assert p!=null && p.host_id!=0 : "PiHost isn't initialized";
		assert p.entities !=null && p.entities.size() > 0 : "entities are empty";
		assert el!=null && el.entity_id!=0 : "Entity is not refreshed yet (" + el + ")";
		
		ResultSet rs;
//		String r;
//		RawRef k;
		List<DiffItem> al = new ArrayList<DiffItem>(100); 
//		rs = prepareStatement("sql_diff02", el.entity_id).executeQuery();
//		while (rs.next()) {
//			r = rs.getString(1);
//			k = new RawRef(r);
//			DiffItem di = new DiffItem(this,p,k.key,rs.getBytes(2),rs.getBytes(3),rs.getLong(4),rs.getLong(5)==1);
//			al.add(di);
//		}
		return al;
	}

	public List<DiffItem> list (PiHost p, Side side, String entname) throws SQLException, IOException, SAXException, InterruptedException, ExecutionException {
		assert p!=null && p.host_id!=0 : "PiHost isn't initialized";
		if (p.entities == null || p.entities.size()==0) {
			refreshMeta(p);
		}
		return list(p, p.getEntity(side, entname));
	}
	
	public HashMap<String,String> readTransportNames(long oref) 
	throws SQLException, UnsupportedEncodingException {
		log.info("Try to read transport names for oref=" + oref);
		PreparedStatement ps = prepareStatement("sql_object_transpnames", oref);
		HashMap<String,String> hm = new HashMap<String,String>(10);
//		ResultSet rs = ps.executeQuery();
//		while (rs.next()) {
//			byte[] oid = rs.getBytes(1), swid = rs.getBytes(5);
//			hm.put("getObjectID", UUtil.getStringUUIDfromBytes(oid) );
//			String s = "/" + rs.getString(3).toLowerCase().substring(0, 3) + "/", n = null;
//			hm.put("getTransportSuffix", s);
//			hm.put("getObjectType", rs.getString(4));
//			hm.put("getObjectSWCV", swid==null ? "" : UUtil.getStringUUIDfromBytes(swid) );
//			Side d = Side.valueOf(rs.getString(3));
//			
//			RawRef r = new RawRef(rs.getString(2));
//			String[] sa = r.key.split("\\|"); 
//			switch (d) {
//				case Repository:
//					assert sa!=null && sa.length==2;
//					n = sa[0];
//					s = sa[1];
//					break;
//				case Directory:
//					assert sa!=null;
//					n = r.key;
//					s = null;
//					break;
//				default:
//					n = "UNKNOWN";
//					s = null;
//			}
//			hm.put("getObjectName", n);
//			hm.put("getObjectNamespace", s);
//			rs.close();
//		}
		ps.close();
		return hm;
	}
	void shutdown() {
		log.info(DUtil.format("shutdown_prepare"));
		HUtil.shutdown();
		log.info(DUtil.format("shutdown"));
	}
	
	// ------------------------------------------------------------------------------
	public List<SWCV> __getSwcvDb(PiHost p, PiEntity ent) throws SQLException {
		List<SWCV> es = new ArrayList<SWCV>(10);
		PreparedStatement ps = prepareStatement("sql_swcvdef_getall", p.host_id)
				, dep = prepareStatement("sql_swcvdeps_getone")
				;
		ResultSet rs = ps.executeQuery(), rd;
		while (rs.next()) {
			SWCV s = new SWCV(ent, rs);
			DUtil.setStatementParams(dep, s.refDB);
			rd = dep.executeQuery();
			s.setDeps(rd);
			es.add(s);
		}
		log.fine("SWCV from database: " + es.size());
		return es;
	}
	public List<PiObject> __getIndexDb(PiHost p, PiEntity ent) throws SQLException {
		List<PiObject> es = new ArrayList<PiObject>(10);
		PreparedStatement obj = prepareStatement("sql_objver_getall", p.host_id, ent.entity_id)
//				, ver = prepareStatement("sql_ver_getlatest")
				;
		ResultSet ro = obj.executeQuery();//, rv;
		while (ro.next()) {
			PiObject o = new PiObject(ent, ro);
//			o.setVersion(rv);
			es.add(o);
		}
		return es;
	}
	/**
	 * Индекс и содержимое SWCV, в т.ч. зависимости
	 * @param p
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 * @throws SQLException
	 * @throws SAXException
	 */
	public void __refreshSWCV(PiHost p, boolean forceOnline) 
		throws IOException, ParseException, SQLException, SAXException, InterruptedException, ExecutionException 
		{
		assert p.entities!=null && p.entities.size()>0 && p.getEntity(Side.Repository, "workspace")!=null;
		PiEntity ent = p.getEntity(Side.Repository, "workspace");

		assert p!=null && ent!=null : "refreshSWCV input" + p + ent;
		List<SWCV> online = null, db = __getSwcvDb(p, ent);

		if (db.size() == 0 || forceOnline) online = p.__getSwcvOnline(ent);

		if (online!=null && online.size()!=0) {
			// сравнение и выравнивание
			PreparedStatement ins = prepareStatement("sql_swcvdef_putone")
					, insdep = prepareStatement("sql_swcvdeps_putone")
					, deldep = prepareStatement("sql_swcvdeps_delprv")
					, ref = prepareStatement("sql_swcvdeps_ref", p.host_id)
					, refset = prepareStatement("sql_swcvdeps_refset")
					;
	
			// 1й проход -- сопоставляем БД и онлайн, проставляем равные
			// поиск от БД
			boolean dirty = false;
			for (SWCV d: db) for (SWCV o: online) if (d.equalAnother(o)) {
				o.refDB = d.refDB;
				log.finest("SWCV-DB " + d + " exists online. Marked as " + d.refDB);
				online.remove(o);
				if (o.deps.size()!=d.deps.size()) {
					log.finest("This " + o + " has dirty deps");
					DUtil.setStatementParams(deldep,o.refDB);
					deldep.addBatch();
					o.setInsertBatchDepFields(insdep);
					dirty = true;
				}
				break;
			}
			// если были изменения в зависимостях, пишем в БД
			if (dirty) {
				DUtil.lock();
				DUtil.executeBatch(deldep);
				DUtil.executeBatch(insdep);
				DUtil.unlock(conn);	
				dirty = false;
			}
			// проход 2 -- пишем новые SWCV 
			if (online.size()>0) {
				// есть что-то ненайденное ранее -- сохраняем
				for (SWCV o: online) if (o.refDB==-1L) {
					o.setInsertFields(ins);
					ins.addBatch();
				}
				DUtil.lock();
				DUtil.executeBatch(ins);
				DUtil.unlock(conn);
				db = __getSwcvDb(p, ent);
				for (SWCV o: online) for (SWCV d: db) if (d.equalAnother(o)) {
					o.refDB = d.refDB;
				}
				for (SWCV o: online) if (o.refDB==-1L) {
					log.severe("This is still unknown SWCV: " + o);
					assert o.refDB!=-1L : "This is still unknown SWCV: " + o;
				}
			}
			HashMap<Long,SWCV> refcacheD = new HashMap <Long,SWCV>(100);
			for (SWCV d: db) refcacheD.put(d.refDB, d);
			for (SWCV o: online) {
				SWCV d = refcacheD.get(o.refDB);  
				assert d!=null;
				if (o.deps.size() != d.deps.size()) {
					log.finest("SWCV " + o + " has dirty deps");
					DUtil.setStatementParams(deldep, o.refDB);
					deldep.addBatch();
					o.setInsertBatchDepFields(insdep);
					dirty = true;
				}
			}
			if (dirty) {
				DUtil.lock();
				DUtil.executeBatch(deldep);
				DUtil.executeBatch(insdep);
				DUtil.unlock(conn);	
				dirty = false;
			}
			ResultSet rs = ref.executeQuery();
			while (rs.next()) {
				DUtil.setStatementParams(refset, 
						rs.getLong(1) == 0 ? null : rs.getLong(1),
						rs.getLong(2),
						rs.getLong(3),
						rs.getBytes(4)
						);
				refset.addBatch();
				dirty = true;
			}
			if (dirty) {
				DUtil.lock();
				DUtil.executeBatch(refset);
				DUtil.unlock(conn);
			}
		}
		p.swcv = new HashMap<Long,SWCV>(db.size());
		for (SWCV d: db) p.swcv.put(d.refDB, d);
	}
	List<PiObject> mergeObjects(PiHost p, PiEntity ent, List<PiObject> db, List<PiObject> online) { 
		List<PiObject> mrgd = new ArrayList<PiObject>(1000);
		for (PiObject o: online) {
			int r = -1;
			PiObject same = null;
			for (PiObject d: db) {
				r = o.equalAnother(d);
				if (r==0) 
					continue;
				same = d;
				break;
			}
			if (same==null) {
				// не найдено -- добавляем из онлайна
				o.kind = Kind.UNKNOWN;
				o.is_dirty = true;
				mrgd.add(o);
			} else if (r==2) {
				// нашли идентичный -- добавляем из базы
				same.kind = Kind.NULL;
				same.is_dirty = false;
				mrgd.add(same);
			} else if (r==1) {
				// объект в онлайне другой версии
				o.previous = same;
				o.kind = Kind.MODIFIED;
				o.is_dirty = true;
				mrgd.add(o);
			}
		}
		return mrgd;
	}
	List<PiObject> updateQueue = new LinkedList<PiObject>();
	synchronized void addPiObjectUpdateQueue(PiObject o) {
		assert o.is_dirty;
		assert !o.inupdatequeue : "Already in queue";
		o.inupdatequeue = true;
		updateQueue.add(o);
	}
	
	void loopUpdateQueue() throws SQLException {
		PreparedStatement insobj = prepareStatement("sql_obj_ins1")
				, insver = prepareStatement("sql_ver_ins1")
				
				, chnobj = prepareStatement("sql_obj_upd2")
				, chnpver = prepareStatement("sql_ver_upd21")
				, chnnver = prepareStatement("sql_ver_ins22")
				;
		HashSet<byte[]> ud1 = new HashSet<byte[]>();
		for (PiObject o: updateQueue) {
			assert o.kind!=Kind.NULL  : "PiObject w/o need of change was added in update queue " + o; 
			assert o.inupdatequeue : "PiObject is already in update queue";
			switch (o.kind) {
				case UNKNOWN:
					assert (!ud1.contains(o.objectid)) : 
						"CONFLICT: duplicate " + o.qryref + "\t" + UUtil.getStringUUIDfromBytes(o.objectid);
					o.refSWCV = o.extrSwcvSp(o.e.host);
					if (o.e.side==Side.Repository && (o.refSWCV==0 || o.refSWCV==-1L)) {
						log.severe("New detected repository object has unknown no reference to SWCV " + o);
						assert false: "New detected repository object has unknown no reference to SWCV " + o;
					}
					DUtil.setStatementParams(insobj, 
							o.e.host.host_id, 
							session_id,
							o.refSWCVsql(),
							o.objectid,
							o.e.entity_id,
							o.qryref,
							o.deleted ? 1 : 0 );
					insobj.addBatch();
					DUtil.setStatementParams(insver, 
							o.e.host.host_id, o.e.entity_id, o.objectid, o.refSWCVsql(), o.versionid, session_id);
					insver.addBatch();
					ud1.add(o.objectid);
					System.out.println("++" + o);
					break;
				case MODIFIED:
					assert o.previous!=null : "Reference to previous object isn't set";
					log.info("Attempt to add object " + o);
					// удаляем объект если не был удалён
					if (o.deleted!=o.previous.deleted) {
						DUtil.setStatementParams(chnobj, o.deleted?1:0);
						chnobj.addBatch();
					}
					// маркируем версию как неактивную
					DUtil.setStatementParams(chnpver, o.previous.refDB, o.previous.versionid, 0);
					chnpver.addBatch();
					// добавляем новую версию
					DUtil.setStatementParams(chnnver, o.previous.refDB, o.versionid, session_id, 1);
					chnnver.addBatch();
				default:
					break;
			}
		}
		// Уррра коротким и максимально пакетным транзакциям!
		DUtil.lock();
		DUtil.executeBatch(insobj);
		DUtil.executeBatch(insver);
		DUtil.executeBatch(chnobj);
		DUtil.executeBatch(chnpver);
		DUtil.executeBatch(chnnver);
		DUtil.unlock(conn);
		
	}
	
	
	@Override
	public boolean refresh(String sid, String url, String user, String password)
			throws MalformedURLException, SQLException, IOException,
			SAXException, ParseException, InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

}
