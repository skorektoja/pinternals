package com.pinternals.diffo;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
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
//	private List<Thread> incoming = new LinkedList<Thread>(), 
//			workers = new LinkedList<Thread>();

	/**
	 * @param dbname путь к файлу БД
	 * @param prx    http-proxy (optional)
	 */
	public Diffo(String dbname, Proxy prx, int tx) {
		proxy = prx;
		dbfile = new File(dbname);
		new DUtil(tx);
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
		DUtil.lock();
		DUtil.unlock(conn);
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
						log.severe(DUtil.format("Diffo02simulatedb_prepare", k, DUtil
								.getSql(k), ex.getMessage()));
					}
				}
			try {
				ok = ok && !cn.isClosed();
				cn.close();
				ok = ok && cn.isClosed();
			} catch (SQLException ex) {
				log.severe(DUtil.format("Diffo03simulatedb_close", ex.getMessage()));
				ok = false;
			}
		}
		if (log.isLoggable(Level.FINER))
			log.finer(t);
		log.log(ok ? Level.CONFIG : Level.SEVERE, DUtil.format("Diffo01simulatedb", ok));
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
	public void refreshMeta(PiHost p, boolean force) throws SQLException, IOException, SAXException, InterruptedException, ExecutionException {
		List<PiEntity> db, online, mrgd = new LinkedList<PiEntity>();
		db = getMetaDB(p, Side.Repository, Side.Directory, Side.SLD);
		log.config(DUtil.format("Diffo10mergeMeta1", "DB", db.size()));
		boolean b = db.isEmpty() || force;
		if (b) {
			online = p.getMetaOnline(Side.Directory, Side.Repository, Side.SLD);
			log.config(DUtil.format("Diffo10mergeMeta1", "ONLINE", online.size()));
			long freshed = mergeMeta(mrgd, db, online);
			if (freshed!=0) mrgd = getMetaDB(p, Side.Directory, Side.Repository, Side.SLD);
		} else
			mrgd = db;
		assert mrgd.size()>0;
		p.entities.clear();
		for (PiEntity e: mrgd)
			p.addEntity(e);
		
		assert p.entities.size()>0 : "No entities retrieved";
		assert p.entities.size()==mrgd.size();
	}

	public List<PiEntity> getMetaDB(PiHost p, Side ... sides) throws SQLException {
		assert p!=null ;
		PreparedStatement psa = prepareStatement("sql_ra_getone")
			, eg = prepareStatement("sql_entities_getside");
		List<PiEntity> db = new ArrayList<PiEntity>(300);
		for (Side side: sides) {
			DUtil.setStatementParams(eg, p.host_id, side.txt());
			ResultSet rs = eg.executeQuery(), rsa;
			while (rs.next()) {
				PiEntity x = new PiEntity(p,rs.getLong(1),side,rs.getString(2),rs.getString(3),rs.getInt(4),rs.getLong(5) == 1);
				rsa = DUtil.setStatementParams(psa, x.entity_id).executeQuery();
				while (rsa.next())
					x.addAttr(rsa.getString(1), rsa.getString(2), rsa.getInt(3));
				// Находим последнее обновление
				x.lastDtFrom = rs.getLong(6);
				x.lastAffected = rs.getLong(7);
				db.add(x);
			}
		}
		return db;
	}
	public long mergeMeta(List<PiEntity> mrgd, List<PiEntity> db, List<PiEntity> online) throws SQLException {
		PreparedStatement inse = prepareStatement("sql_entities_ins")
				, insa = prepareStatement("sql_ra_ins");
		int freshed=0;
		for (PiEntity x: online) {
			PiHost p = x.host;
			int q = db.indexOf(x);
			if (q == -1) {
				// Есть онлайн, нету в БД => добавляем
				// INSERT INTO entity(side,internal,host_id,caption,seqno,session_id,is_indexed) VALUES (?1,?2,?3,?4,?5,?6,?7);
				DUtil.setStatementParams(inse,x.side.txt(),x.intname,p.host_id,x.title,x.seqno,session_id,
						x.is_indexed ? 1 : 0);
				inse.addBatch();
				for (ResultAttribute a: x.attrs) {
					// добавляем и атрибуты
					// NSERT INTO ra(entity_id,raint,racaption,seqno) VALUES (?1,?2,?3,?4);
					DUtil.setStatementParams(insa,x.side.txt(),x.intname,p.host_id,a.internal,a.caption,a.seqno);
					insa.addBatch();
				}
				mrgd.add(x);
				freshed++;
			} else {
				assert x.attrs.size()==db.get(q).attrs.size() : "Attributes were changed. System upgraded?";
				mrgd.add(x);
			}
		}
		if (freshed>0) {
			DUtil.lock();
			inse.executeBatch();
			insa.executeBatch();
			DUtil.unlock(conn);
		}
		return freshed;
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
			refreshMeta(p, false);
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
		DUtil.shutdown();
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
	public void refreshSWCV(PiHost p, boolean forceOnline) 
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
		List<PiObject> mrgd = new ArrayList<PiObject>(10000);
		for (PiObject o: online) {
			assert o.e == ent;
			int r = -1;
			PiObject sameO = null, sameV = null;
			for (PiObject d: db) {
				r = o.equalAnother(d);
				sameO = r==2 ? d : sameO;
				sameV = r==1 ? d : sameV;
				if (r==0) 
					continue;
				else if (r==2)
					break;
			}
			if (sameV==null && sameO==null) {
				// не найдено -- добавляем из онлайна
				o.kind = Kind.UNKNOWN;
				o.is_dirty = true;
				mrgd.add(o);
			} else if (sameO!=null) {
				// нашли идентичный -- добавляем из базы
				sameO.kind = Kind.NULL;
				sameO.is_dirty = false;
				mrgd.add(sameO);
			} else if (sameV!=null) {
				// объект в онлайне другой версии
				o.previous = sameV;
				o.kind = Kind.MODIFIED;
				o.is_dirty = true;
				mrgd.add(o);
			} 
		}
		return mrgd;
	}
//	List<PiObject> updateQueue = new LinkedList<PiObject>();
//	synchronized void addPiObjectUpdateQueue(PiObject o) {
//		assert o.is_dirty;
//		assert !o.inupdatequeue : "Already in queue";
//		o.inupdatequeue = true;
//		updateQueue.add(o);
//	}
	
	void loopUpdateQueue(List<PiObject> updateQueue) throws SQLException, MalformedURLException, IOException, InterruptedException, ExecutionException {
		// Массовая вставка
		PreparedStatement insobj = prepareStatement("sql_obj_ins1")
				, insver = prepareStatement("sql_ver_ins1")	// insver через SWCV_REF
				, insver2 = prepareStatement("sql_ver_ins2")	// insver без SWCV_REF
				
				, chnobj = prepareStatement("sql_obj_upd2")
				, chnpver = prepareStatement("sql_ver_upd21")
				, chninsver = prepareStatement("sql_ver_ins22")
				, getref1 = prepareStatement("sql_obj_getref1")
				, getref2 = prepareStatement("sql_obj_getref2")
				;
		HashSet<byte[]> ud1 = new HashSet<byte[]>();
		final Long l1 = new Long(1), l0 = new Long(0);
		long z=0, zz=0;
		log.info("Objects in update queue: " + updateQueue.size());
		
		List<PiObject> needPayloads = new LinkedList<PiObject>();
		
		for (PiObject o: updateQueue) {
			assert o.kind!=Kind.NULL  : "PiObject w/o need of change was added in update queue " + o; 
			assert o.inupdatequeue : "PiObject is already in update queue";
			switch (o.kind) {
				case UNKNOWN:
					assert (!ud1.contains(o.objectid)) : 
						"CONFLICT: duplicate " + o.qryref + "\t" + UUtil.getStringUUIDfromBytes(o.objectid);
					DUtil.setStatementParams(insobj, 
							o.e.host.host_id, 
							session_id,
							o.refSWCVsql(),
							o.objectid,
							o.e.entity_id,
							o.qryref,
							o.deleted ? l1 : l0 );
					insobj.addBatch();
					if (o.refSWCV!=-1L) {
						DUtil.setStatementParams(insver, 
							o.e.host.host_id, o.e.entity_id, o.objectid, o.refSWCV, o.versionid, session_id);
						insver.addBatch();
					} else {
						DUtil.setStatementParams(insver2, 
								o.e.host.host_id, o.e.entity_id, o.objectid, o.versionid, session_id);
						insver2.addBatch();
					}
					ud1.add(o.objectid);
					z++;
					if (!o.deleted) needPayloads.add(o);
					break;
				case MODIFIED:
					assert o.previous!=null : "Reference to previous object isn't set";
					assert o.previous.refSWCV == o.refSWCV : 
						"refSWCV changed from previous state. Was " + o.previous.refSWCV + " now is " + o.refSWCV + ", object " + o;

					log.info("Attempt to add object " + o);
					// удаляем объект если не был удалён
					if (o.deleted!=o.previous.deleted) {
						DUtil.setStatementParams(chnobj, o.deleted?1:0);
						chnobj.addBatch();
					}
					// маркируем версию как неактивную
					DUtil.setStatementParams(chnpver, o.previous.refDB, o.previous.versionid, l0);
					chnpver.addBatch();
					// добавляем новую версию
					DUtil.setStatementParams(chninsver, o.previous.refDB, o.versionid, session_id, l1);
					chninsver.addBatch();
					z++;
					if (!o.deleted) needPayloads.add(o);
					break;
				default:
					break;
			}
//			System.out.print(".");
		}

		// Уррра коротким и максимально пакетным транзакциям!
		DUtil.lock();
		DUtil.executeBatch(insobj);
		DUtil.executeBatch(insver);
		DUtil.executeBatch(insver2);
		DUtil.executeBatch(chnobj);
		DUtil.executeBatch(chnpver);
		DUtil.executeBatch(chninsver);
		saveStatistic(updateQueue);
		DUtil.unlock(conn);

		String sen = null; 
		PiHost p = null; 
		long pd=0;
		for (PiObject o: needPayloads) {
			assert o.inupdatequeue;
			ResultSet rs = null;
			if (o.refDB==-1L && o.refSWCV!=-1L) {
				// need to retrieve updated reference to repository object, for payload link
				DUtil.setStatementParams(getref1, o.e.host.host_id, o.refSWCV, o.objectid, o.e.entity_id);
				rs = getref1.executeQuery();
			} else if (o.refDB==-1L && o.refSWCV==-1L) {
				// directory or other object
				DUtil.setStatementParams(getref2, o.e.host.host_id, o.objectid, o.e.entity_id);
				rs = getref2.executeQuery();
			} else {
				assert false: "Unpredicted state: referenceDB=" + o.refDB ;
			}
			o.inupdatequeue = false;
			while (rs.next()) {
				o.refDB = rs.getLong(1);
				break;
			}
			if (o.refDB==-1L) {
				log.severe("\n\n\nXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXxxxx //TODO\n\n\n"); //TODO
			} else {
//				System.out.println("New object to download " + o);
				o.e.host.downloadObject(o);
				pd += o.ftask!=null ? 1 : 0;
				sen = sen==null ? o.e.toString() : sen;
				p = p==null ? o.e.host : p;
			}
		}
		
		while (pd>0) {
			System.out.println(sen + "|yet in payload queue: " + pd);
			for (PiObject o: needPayloads) if (o.ftask!=null) {
				HTask h = o.ftask.get();
				if (h.ok) {
//					if (h.attempts>0) log.config("");
					o.e.host.updatePayload(o, h);
					pd--;
				} else if (!h.ok && h.attempts<10){
					HttpURLConnection c = o.e.host.establishGET(new URL(o.qryref), true);
					o.ftask = DUtil.addHTask(h.reset(c));
					System.out.println("Error when download " + h + " attempts=" + h.attempts);
				} else if (!h.ok)
					pd--;
			}
			p.commitHostDb();
		}
		if (p!=null) p.commitHostDb();
		zz += z;
		log.info("Objects handled total: " + zz);
	}
	public void saveStatistic(List<PiObject> updateQueue) throws SQLException {
		HashSet<PiEntity> hse = new HashSet<PiEntity>(100);  
		for (PiObject o: updateQueue) {
			o.e.incAffected();
			hse.add(o.e);
		}
		PreparedStatement pse = prepareStatement("sql_upd_qryst");
		for (PiEntity e: hse) {
			System.out.println("Save statistic: " + e + " affected=" + e.affected);
			DUtil.setStatementParams(pse, session_id, e.host.host_id,e.entity_id,e.minDT,e.affected);
			pse.addBatch();
		}
		DUtil.executeBatch(pse);
		for (PiEntity e: hse) {
			e.affected = 0;
		}
	}
	public void cleanobjver() throws SQLException {
		PreparedStatement ps1 = DUtil.prepareStatement(conn, "sql_cleanobjver1"),
				ps2 = DUtil.prepareStatement(conn, "sql_cleanobjver2"),
				ps3 = DUtil.prepareStatement(conn, "sql_cleanobjver3");
		DUtil.lock();
		ps1.executeUpdate();
		ps2.executeUpdate();
		ps3.executeUpdate();
		DUtil.unlock(conn);
	}
	
	@Override
	public boolean refresh(String sid, String url, String user, String password)
			throws RuntimeException
			 {
		try {
			PiHost pih = addPiHost(sid, url);
			pih.setUserCredentials(user, password);
			HierRoot root =  new HierRoot(this,pih);
			refreshMeta(pih, false);
			refreshSWCV(pih, false);

			root.addSide(Side.Repository);
			root.addSide(Side.Directory);

			for (HierSide s: root.sides) 
				for (PiEntity v: pih.entities.values())
					if (v.side == s.side) {
						HierEnt he = s.addPiEntity(v);
						he.getObjectsIndex(false);
					}
		} catch (Exception ce) {
			throw new RuntimeException(ce);
		}
		return true;
	}

}
