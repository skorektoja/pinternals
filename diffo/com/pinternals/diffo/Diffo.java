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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.xml.sax.SAXException;

import com.pinternals.diffo.api.IDiffo;

/**
 * @author Илья Кузнецов Обращается к БД, ведёт сессию и хранит список хостов,
 *         должен быть синглтоном. Также отдаёт назад результаты сравнений.
 */
public class Diffo implements IDiffo, Cloneable {
	private static Logger log = Logger.getLogger(Diffo.class.getName());
	public static String version = "0.1.0";
	private ArrayList<PiHost> pihosts = new ArrayList<PiHost>(10);
	private Connection conn = null;
	private File dbfile = null;

	public Long session_id = -1L;
	public Proxy proxy;

	/**
	 * @param dbname путь к файлу БД
	 * @param prx    http-proxy (optional)
	 */
	public Diffo(String dbname, Proxy prx) {
		proxy = prx;
		dbfile = new File(dbname);
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
	private PreparedStatement prepareStatement(String key, Object... objs)
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
			for (String s: DUtil.sqlKeySet) if (s.startsWith("hosql_init")) {
				log.config(s);
				DUtil.prepareStatement(hostcon, s).executeUpdate();
				hostcon.commit();
			}
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
	public void refreshMeta(PiHost p) throws SQLException, IOException, SAXException {
		refreshMeta(p, Side.Repository);
		refreshMeta(p, Side.Directory);
		refreshMeta(p, Side.SLD);
	}

	public void refreshMeta(PiHost p, Side side) throws SQLException, IOException, SAXException {
		boolean forceonline = false;
		
		PreparedStatement inse = prepareStatement("sql_entities_ins")
			, insa = prepareStatement("sql_ra_ins")
			, psa = prepareStatement("sql_ra_getone")
			, eg = prepareStatement("sql_entities_getside", p.host_id, side.txt()); 
		ResultSet rs = eg.executeQuery(), rsa;
		int i, q, c=0;
		ArrayList<PiEntity> db = new ArrayList<PiEntity>(100), online = new ArrayList<PiEntity>(0);

		// Из БД берём ВСЁ
		while (rs.next()) {
			PiEntity x = new PiEntity(rs.getLong(1),side,rs.getString(2),rs.getString(3),rs.getInt(4));
			rsa = DUtil.setStatementParams(psa, x.entity_id).executeQuery();
			while (rsa.next())
				x.attrs.add(new ResultAttribute(rsa.getString(1), rsa.getString(2), rsa.getInt(3)));
			db.add(x);
		}
		if (forceonline || db.isEmpty()) online = p.collectDocsRA(side);
		for (PiEntity x: online) {
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
				c++;
				p.addEntity(x);
			} else {
				assert x.attrs.size()==db.get(q).attrs.size();
				p.addEntity(x);
				db.remove(q);
			}
		}
		for (PiEntity x: db) 
			p.addEntity(x);   // в online нет (пропало?) или не запрашивалось (forceonline=false) 
		
		commit();
		assert !forceonline || db.isEmpty() : "there are " + db.size() + " entities unknown: " + side + " at " + p.sid;
		assert validatedb() : "DB is invalid"; 
	}

	public boolean refreshSWCV(PiHost p) 
		throws IOException, ParseException, SQLException, SAXException 
		{ 
		// TODO: implement SWCV changes and	re-orderings 
		ArrayList<SWCV> as = p.askSWCV(); 
		boolean ok = true; 
		assert as != null : "SWCV extraction problem"; 
		PreparedStatement ps = prepareStatement("sql_swcv_getone");
		PreparedStatement ins = prepareStatement("sql_swcv_putone"); 
		long found = 0, notfound = 0; 
		for (SWCV s : as) { 
			DUtil.setStatementParams(ps, p.host_id, s.ws_id, s.sp, s.seqno); 
			ResultSet r = ps.executeQuery();
			if (r.next()) { 
				s.ref = r.getLong(1); 
				long l = r.getLong(2); 
				assert l == 0 || l == 1 : "unknown index_objects SWCV ref=" + s.ref; 
				s.index_me = r.getLong(2) == 1; 
				found++; 
			} else { 
				notfound++; // find indexation settings depends on vendor 
				s.index_me = true; //s.vendor.equals("sap.com") ? p.ime_sapcom : p.ime_customer; 
				// 1..9 =
				// host_id,session_id,ws_id,type,vendor,caption,name,sp,seqno
				// 10..14 =
				// modify_date,modify_user,dependent_id,is_editable,is_original
				DUtil.setStatementParams(ins, p.host_id, session_id, s.ws_id, s.type,
						s.vendor, s.caption, s.name, s.sp, s.seqno,
						s.modify_date, s.modify_user, s.dependent_id,
						s.is_editable ? 1 : 0, s.is_original ? 1 : 0,
						s.index_me ? 1 : 0);
				ins.addBatch();
			}
		}
		assert found + notfound == as.size() : "size are mismatches";
		if (notfound > 0) {
			int bi[] = ins.executeBatch(), nf2=0;
			commit();
			for (int i = 0; i < bi.length; i++)
				nf2 += bi[i];
			ok = ok && notfound == nf2;
			assert notfound == nf2 : "not all the SWCV are updated";
			// for getting ref, we are looking for unknown references
			for (SWCV s : as)
				if (s.is_unknown()) {
					DUtil.setStatementParams(ps, p.host_id, s.ws_id, s.sp, s.seqno);
					ResultSet r = ps.executeQuery();
					ok = ok && r != null;
					assert r != null : "SWCV isn't found after insert";
					s.ref = r.getLong(1);
					nf2--;
				}
			ok = ok && nf2 == 0;
			assert nf2 == 0 : "not all the SWCV are with references yet";
		}
		p.swcv = new HashMap<Long,SWCV>(as.size());
		for (SWCV s: as) p.swcv.put(s.ref, s);
		assert validatedb();
		return ok;
	}

	public void askIndexRepository(PiHost p) throws IOException, SQLException, SAXException {
		Side r = Side.Repository;
		PiEntity[] reps = {p.getEntity(r, "namespdecl"),
				p.getEntity(r, "ifmtypedef"),			// Data type
				p.getEntity(r, "XI_TRAFO"),				// message mapping
				p.getEntity(r, "AdapterMetaData"),
				p.getEntity(r, "ifmcontobj"),
				p.getEntity(r, "ifmtypeenh"),
				p.getEntity(r, "ifmextdef"),
				p.getEntity(r, "ifmextmes"),
				p.getEntity(r, "ifmfaultm"),
				p.getEntity(r, "FUNC_LIB"),
				p.getEntity(r, "FUNC_LIB_PROG"),
				p.getEntity(r, "ChannelTemplate"),
				p.getEntity(r, "MAP_ARCHIVE_PRG"),
				p.getEntity(r, "MAPPING"),
				p.getEntity(r, "AlertCategory"),
				p.getEntity(r, "TRAFO_JAR"),
				p.getEntity(r, "RepBProcess"),
				p.getEntity(r, "rfc"),
				p.getEntity(r, "idoc"),
				p.getEntity(r, "imsg"),
				p.getEntity(r, "iseg"),
				p.getEntity(r, "ityp"),
				p.getEntity(r, "ifmclsfn"),
				p.getEntity(r, "ifmmessage"),
				p.getEntity(r, "ifmoper"),
				p.getEntity(r, "processcomp"),
				p.getEntity(r, "process"),
				p.getEntity(r, "rfcmsg"),
				p.getEntity(r, "ifmmessif"),
				p.getEntity(r, "MAPPING_TEST"),
				p.getEntity(r, "DOCU"),
				p.getEntity(r, "arismodelext"),
				p.getEntity(r, "arisprofile"),
				p.getEntity(r, "ariscxnocc"),
				p.getEntity(r, "arisobjocc"),
				p.getEntity(r, "aristextocc"),
//				p.getEntity(r, ""),
//				p.getEntity(r, ""),
			};
		for (PiEntity e: reps) {
			handleIndexRepositoryObjectsVersions(p.askIndex(e), p, e);
		}
	}
	
	public void askIndexDirectory(PiHost p) throws SQLException, IOException, SAXException {
		Side d = Side.Directory;
		PiEntity[] dirs = {
				p.getEntity(d, "Party"),
				p.getEntity(d, "Service"),
				p.getEntity(d, "Channel"),
				p.getEntity(d, "InboundBinding"),
				p.getEntity(d, "OutboundBinding"),
				p.getEntity(d, "RoutingRelation"),
				p.getEntity(d, "RoutingRule"),
				p.getEntity(d, "MappingRelation"),
				p.getEntity(d, "P2PBinding"),
				p.getEntity(d, "DirectoryView"),
				p.getEntity(d, "ValueMapping"),
				p.getEntity(d, "DOCU"),
//				p.getEntity(d, "AgencySchemObj"),	// involve http 500
//				p.getEntity(d, ""),
//				p.getEntity(d, ""),
		};
		for (PiEntity e: dirs)
			handleIndexDirectoryObjectsVersions(p.askIndex(e), p, e);
	}

	private void fill_tmp3(ArrayList<PiObject> a) throws SQLException {
		prepareStatement("sql_tmp3_del").execute();
		PreparedStatement ins = prepareStatement("sql_tmp3_ins");
		assert a!=null;
		for (PiObject o: a) {
			DUtil.setStatementParams(ins, o.objectid, o.versionid, o.deleted?1:0);
			ins.addBatch();
		}
		ins.executeBatch();
		commit();
	}

	private void handleIndexDirectoryObjectsVersions(ArrayList<PiObject> objs, PiHost p, PiEntity e)
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
		fill_tmp3(objs);
		
		// делаем искалку. Так только для Directory! в Repository будет +SWCV и +SP
		HashMap<ByteBuffer, PiObject> hm = new HashMap<ByteBuffer, PiObject>(objs.size());
		for (PiObject o: objs) {
			hm.put(ByteBuffer.wrap(o.objectid), o);
		}

		ResultSet rs = sel.executeQuery();
		assert rs!=null;
		int i;

		while (rs.next()) {
			String txt = rs.getString(1);
			byte[] oid = rs.getBytes(2), vid=rs.getBytes(3);
			long oref = rs.getLong(4);
			
			PiObject o = hm.get(ByteBuffer.wrap(oid));
			assert o!=null : "UNKNOWN OBJECT reference " + oref;
			assert UUtil.areEquals(o.versionid,vid);
			assert UUtil.areEquals(o.objectid,oid);
			
			if (txt.equals("CURRENT_LIVE") || txt.equals("CURRENT_DEAD")) 
				o.refDB = oref;
			if (txt.equals("NEWVER_LIVE")) {
				o.refDB = oref;
				// появилась новая версия объекта. 
//				zip = DUtil.readHttpConnection(p.establishGET(new URL(o.rawref), true));
				// Деактивируем старую версию, сессия не меняется
				i = DUtil.setStatementParams(deactV,oref).executeUpdate();
				assert i==1 : "deactivate failed, rows updated:"+i;
				// добавляем новую активную версию 
				i = DUtil.setStatementParams(insV, o.refDB, o.versionid, session_id, 1).executeUpdate();
				assert i==1 : "insert version touched rows: " + i;
				p.addObject(o, session_id, true);
			} else if (txt.equals("NEWVER_DEAD")) {
				o.refDB = oref;
				// появилась новая версия объекта и она удалена.
				i = DUtil.setStatementParams(insV, o.refDB, o.versionid, session_id, 0).executeUpdate();
				assert i==1 : "insert version touched rows: " + i;
				i = DUtil.setStatementParams(del, o.refDB, session_id).executeUpdate();
				assert i==1 : "update failed, rows:" + i;
			} else if (txt.equals("NEWOBJECT")) {
				// объект может быть удалённым, но о нём мы узнаём впервые.
//				zip = o.deleted? null: DUtil.readHttpConnection(p.establishGET(new URL(o.rawref), true));
				i = DUtil.setStatementParams(ins,p.host_id,session_id,o.objectid,o.e.entity_id,o.rawref,o.deleted?1:0).executeUpdate();
				assert i==1 : "insert object failed, rows touched:"+i;
				o.refDB = ins.getGeneratedKeys().getLong(1);
				// добавляем новую активную версию
				i = DUtil.setStatementParams(insV,o.refDB,o.versionid,session_id,o.deleted?0:1).executeUpdate();
				if (!o.deleted) p.addObject(o, session_id, true);
			}
		}
		p.addObjectCommit(true);
		commit();
		i = 0;
		for (PiObject o: objs) if (o.refDB<1){
			i++;
		}
		assert i==0 : "" + i + " objects are not handled; all amount is " + objs.size() ;
		assert validatedb();
	}

	private void fill_tmp7(ArrayList<PiObject> a, PiHost p) throws SQLException {
		prepareStatement("sql_tmp7_del").execute();
		PreparedStatement ins = prepareStatement("sql_tmp7_ins"), sel=prepareStatement("sql_tmp7_idx");
		assert a!=null;
		int i=0;
		for (PiObject o: a) {
			Object []x = o.extrSwcvSp();
			// (oid,vid,swcv,sp,del,host_id)
			DUtil.setStatementParams(ins, o.objectid, o.versionid, x[0], x[1], o.deleted?1:0, p.host_id, i++);
			ins.addBatch();
		}
		ins.executeBatch();
		commit();
		ResultSet rs = sel.executeQuery();
		i = 0;
		while (rs.next()) {
			a.get(i).refSWCV = rs.getLong(1);
			assert a.get(i).refSWCV!=0;
			assert i==rs.getLong(2);
			i++;
		}
		rs.close();
	}

	private void handleIndexRepositoryObjectsVersions(ArrayList<PiObject> objs, PiHost p, PiEntity e)
	throws SQLException {
		assert objs!=null && p!=null && e!=null;
		PreparedStatement sel = prepareStatement("sql_objrep_report", e.entity_id)
			, del = prepareStatement("sql_objrep_del")
			, deactV = prepareStatement("sql_ver_deactv")
			, ins = prepareStatement("sql_objrep_ins")
			, insV = prepareStatement("sql_ver_ins")
			;
		boolean ignore_sap_deleted = true;	// true для игнорирования
		boolean ignore_sap_alive = true;   // true для игнорирования
		
		// Вставить гуиды, полученные онлайн, в tmp7 И УЗНАТЬ ССЫЛКИ REF
		fill_tmp7(objs, p);
		
		// делаем искалку
		HashMap<ByteBuffer,ArrayList<PiObject>> hm = new HashMap<ByteBuffer,ArrayList<PiObject>>(objs.size());
		for (PiObject o: objs) {
			ByteBuffer b = ByteBuffer.wrap(o.objectid);
			ArrayList<PiObject> t = hm.get(b);
			if (t==null) {
				t = new ArrayList<PiObject>(2);
				t.add(o);
				hm.put(b,t);
			} else
				t.add(o);
		}

		ResultSet rs = sel.executeQuery();
		assert rs!=null;
		int i, sapdeleted=0, sapalive=0;
		SWCV swcv = null;

		while (rs.next()) {
			String txt = rs.getString(1);
			byte[] oid = rs.getBytes(2), vid=rs.getBytes(3);

			long swcref=rs.getLong(4), oref = rs.getLong(5);
			ArrayList<PiObject> ol = hm.get(ByteBuffer.wrap(oid));
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
//			System.out.println("#" + txt + " object " + oref);
//			System.out.println(o==null ? " null " : o );
			if (txt.equals("CURRENT_LIVE") || txt.equals("CURRENT_DEAD")) 
				o.refDB = oref;
			else if (txt.equals("NEWOBJECT")) {
				// объект может быть удалённым, но о нём мы узнаём впервые.
				if (o.deleted && swcv.is_sap() && ignore_sap_deleted) {
					// Не стоит записывать удалённые саповские объекты, они замусоривают базу
					sapdeleted++;
				} else if (!o.deleted && swcv.is_sap() && ignore_sap_alive) {
					// Не стоит записывать удалённые саповские объекты, они замусоривают базу
					sapalive++;
				} else {
//					zip = o.deleted? null: p.readHttpConnection(p.establishGET(new URL(o.rawref), true));
					i = DUtil.setStatementParams(ins,p.host_id,session_id,o.refSWCV,o.objectid,o.e.entity_id,o.rawref,o.deleted?1:0).executeUpdate();
					assert i==1: "insertion REP object failed, rows affected:"+i;
					o.refDB = ins.getGeneratedKeys().getLong(1);
					assert o.refDB!=0 : "bad object_ref:" + o.refDB;
					i = DUtil.setStatementParams(insV, o.refDB, o.versionid, session_id, o.deleted?0:1).executeUpdate();
					assert i==1: "insertion REP version failed, rows affected:"+i;
					if (!o.deleted) p.addObject(o, session_id, true);
				}
			} else if (txt.equals("NEWVER_LIVE")) {
				o.refDB = oref;
				// появилась новая версия объекта. 
//				zip = DUtil.readHttpConnection(p.establishGET(new URL(o.rawref),true));
				// Деактивируем старую версию, сессия не меняется
				i = DUtil.setStatementParams(deactV,oref).executeUpdate();
				assert i==1 : "deactivate failed, rows updated:"+i;
				// добавляем новую активную версию
				i = DUtil.setStatementParams(insV, o.refDB, o.versionid, session_id, 1).executeUpdate();
				assert i==1 : "insert version touched rows: " + i;
				p.addObject(o, session_id, true);
			} else if (txt.equals("NEWVER_DEAD")) {
				assert !swcv.is_sap();
				o.refDB = oref;
				// появилась новая версия объекта и она удалена.
				i = DUtil.setStatementParams(insV, o.refDB, o.versionid, session_id, 0, null).executeUpdate();
				assert i==1 : "insert version touched rows: " + i;
				i = DUtil.setStatementParams(del, o.refDB, session_id).executeUpdate();
				assert i==1 : "update failed, rows:" + i;
			} else {
				i = 0/0;
			}
		}
		i = -sapdeleted - sapalive;
		for (PiObject o: objs) if (o.refDB<1){
			i++;
		}
		assert i==0: i+" objects are not handled; all amount is " + objs.size() + "; db rolled back:" + rollback();
		p.addObjectCommit(true);
		commit();
		assert validatedb();
	}

	public void askSld(PiHost p) throws IOException, SAXException {
		Side l = Side.SLD;
		PiEntity slds[] = {
				p.getEntity(l, "SAP_BusinessSystem"),
				p.getEntity(l, "SAP_BusinessSystemGroup"),
				p.getEntity(l, "SAP_BusinessSystemPath"),
		};
		for (PiEntity e: slds) {
			ArrayList<PiObject> objs = p.askIndex(e);
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
	
	public void fullFarsch(PiHost p) 
	throws SQLException, IOException, SAXException, ParseException {
		boolean b = true;
		int i = 10;
		p.download(false);
//		while (!p.download(false) && --i > 0) ;
//		if (true) return;

		if (b && p.isSideAvailable(Side.Repository)) {
			refreshMeta(p,Side.Repository);
			refreshSWCV(p);
			if ((p.swcv.size()>0)) askIndexRepository(p);
		}
		if (b && p.isSideAvailable(Side.Directory)) {
			refreshMeta(p, Side.Directory);
			askIndexDirectory(p);
		}
		if (b && p.isSideAvailable(Side.SLD)) {
			refreshMeta(p, Side.SLD);
			askSld(p);
		}
		p.download(true);
	}

	public ArrayList<DiffItem> list (PiHost p, PiEntity el) throws SQLException, IOException {
		assert p!=null && p.host_id!=0 : "PiHost isn't initialized";
		assert p.entities !=null && p.entities.size() > 0 : "entities are empty";
		assert el!=null && el.entity_id!=0;
		
		ResultSet rs;
		String r;
		RawRef k;
		ArrayList<DiffItem> al = new ArrayList<DiffItem>(100); 
		rs = prepareStatement("sql_diff02", el.entity_id).executeQuery();
		while (rs.next()) {
			r = rs.getString(1);
			k = new RawRef(r);
			DiffItem di = new DiffItem(this,p,k.key,rs.getBytes(2),rs.getBytes(3),rs.getLong(4),rs.getLong(5)==1);
			al.add(di);
		}
		return al;
	}

	public ArrayList<DiffItem> list (PiHost p, Side side, String entname) throws SQLException, IOException, SAXException {
		assert p!=null && p.host_id!=0 : "PiHost isn't initialized";
		if (p.entities == null || p.entities.size()==0) {
			refreshMeta(p, Side.Repository);
			refreshMeta(p, Side.Directory);
			refreshMeta(p, Side.SLD);
		}
		return list(p, p.getEntity(side, entname));
	}
	
	@Override
	public boolean refresh(String sid, String url, String user, String password)
	throws MalformedURLException, SQLException, IOException, SAXException, ParseException {
		PiHost p = addPiHost(sid, url);
		p.setUserCredentials(user, password);
		fullFarsch(p);
		return true;
	}


	public HashMap<String,String> readTransportNames(long oref) 
	throws SQLException, UnsupportedEncodingException {
		log.info("Try to read transport names for oref=" + oref);
		PreparedStatement ps = prepareStatement("sql_object_transpnames", oref);
		HashMap<String,String> hm = new HashMap<String,String>(10);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			byte[] oid = rs.getBytes(1), swid = rs.getBytes(5);
			hm.put("getObjectID", UUtil.getStringUUIDfromBytes(oid) );
			String s = "/" + rs.getString(3).toLowerCase().substring(0, 3) + "/", n = null;
			hm.put("getTransportSuffix", s);
			hm.put("getObjectType", rs.getString(4));
			hm.put("getObjectSWCV", swid==null ? "" : UUtil.getStringUUIDfromBytes(swid) );
			Side d = Side.valueOf(rs.getString(3));
			
			RawRef r = new RawRef(rs.getString(2));
			String[] sa = r.key.split("\\|"); 
			switch (d) {
				case Repository:
					assert sa!=null && sa.length==2;
					n = sa[0];
					s = sa[1];
					break;
				default: 			
					n = "";
					s = "";
			}
			hm.put("getObjectName", n);
			hm.put("getObjectNamespace", s);
			rs.close();
		}
		ps.close();
		return hm;
	}

}
