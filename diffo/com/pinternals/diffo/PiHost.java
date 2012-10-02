package com.pinternals.diffo;

import java.io.IOException;
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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.xml.sax.SAXException;

import sun.misc.BASE64Encoder;

public class PiHost {
	private static Logger log = Logger.getLogger(Diffo.class.getName());

	/** Для каждого хоста может быть свой прокси, поэтому пусть будет здесь */
	public String basicAuth=null, uname=null, passwd=null;
	private String action = null;
	private Object actobjs = null;
	public String sid = null;
	public Long host_id = new Long(-1);
	protected Diffo diffo = null;
	protected Connection hostdb = null;
	String hostdbfn = null; 
	private PreparedStatement psInboxIns=null, psInboxDel=null, psGP = null; 
	private static final Lock lock = new ReentrantLock();

	public URL uroot;
	private URL ucpahtml, uaf_schxml;
//	private CPACache cpacache = null;
	private int timeoutMillis=30000;
	private Proxy proxy = null;

	public HashMap<String,PiEntity> entities = new HashMap<String,PiEntity>(20);
//	public boolean ime_sapcom = false, ime_customer = false;

	public HashMap<Long,SWCV> swcv = null;

	public PiHost (Diffo d, String sid, String sURL, long host_id, Connection hostdb) 
	throws MalformedURLException, SQLException {
		this.proxy = d.proxy;
		this.diffo = d;
		this.sid = sid;
		this.uroot = new URL(sURL);
//		this.ucpahtml = new URL(sURL + Side.C_CPA_S);
//		this.uaf_schxml = new URL(sURL + Side.C_AF_SCH_XML);
		this.host_id = host_id;
		this.hostdb = hostdb;
		this.hostdbfn = "";
		psGP = DUtil.prepareStatement(hostdb, "hosql_getpayload");
		timeoutMillis=30000;
		log.config("TODO: add config for PiHost"); //TODO
	}
	public void close() throws SQLException {
		this.hostdb.close();
	}
	public void setUserCredentials(String uname, String passwd) {
		this.uname = uname;
		this.passwd = passwd;
		String token = uname + ":" + passwd;
		basicAuth = "Basic " + new String(new BASE64Encoder().encode(token.getBytes()));
	}
	// -------------------------------------- HTTP services
	protected HttpURLConnection establishPOST(URL u, boolean useCredentials) throws MalformedURLException, IOException {
		HttpURLConnection huc = DUtil.getHttpConnection(proxy, u, timeoutMillis);
		huc.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
		huc.setRequestMethod("POST");
		if (useCredentials) huc.setRequestProperty("Authorization", basicAuth);
		return huc;
	}
	protected HttpURLConnection establishGET(URL u, boolean useCredentials) 
	throws IOException {
		HttpURLConnection huc = DUtil.getHttpConnection(proxy, u, timeoutMillis);
		huc.setRequestMethod("GET");
		if (useCredentials) huc.setRequestProperty("Authorization", basicAuth);
		return huc;
	}
	
	// --------------------------------------------------------- XI services
	private boolean buildSLD (Side sld, List<PiEntity> es) {
//		int q=0;
//		PiEntity bs = new PiEntity(this, -1, sld, "SAP_BusinessSystem", "Business System", q++);
//		bs.ok = true;
//		int r = 0;
//		bs.attrs.add(new ResultAttribute("Name", "Name", r++));
//		bs.attrs.add(new ResultAttribute("Caption", "Caption", r++));
//		bs.attrs.add(new ResultAttribute("SAP_CreationTime", "CreatedAt", r++));
//		bs.attrs.add(new ResultAttribute("SAP_LastChangedBy", "ChangedBy", r++));
//		bs.attrs.add(new ResultAttribute("SAP_LastModificationTime", "ModifiedAt", r++));
//		bs.attrs.add(new ResultAttribute("SAP_GlobalUniqueID", "GUID", r++));
//		es.add(bs);
//		
//		PiEntity ag = new PiEntity(this, -1, sld, "SAP_BusinessSystemGuid", "SAP_BusinessSystemGuid", q++);
//		ag.ok = true;
//		r=0;
//		ag.attrs.add(new ResultAttribute("SAP_CreationTime", "CreatedAt", r++));
//		ag.attrs.add(new ResultAttribute("SAP_LastChangedBy", "ChangedBy", r++));
//		ag.attrs.add(new ResultAttribute("SAP_LastModificationTime", "ModifiedAt", r++));
//		ag.attrs.add(new ResultAttribute("SAP_GlobalUniqueID", "SAP_GlobalUniqueID", r++));
//		ag.attrs.add(new ResultAttribute("SAP_LogicalBusinessSystem", "SAP_LogicalBusinessSystem", r++));
//		es.add(ag);
//		
//		PiEntity bsg = new PiEntity(this, -1, sld, "SAP_BusinessSystemGroup", "Business System Group", q++);
//		bsg.ok = true;
//		r = 0;
//		bsg.attrs.add(new ResultAttribute("Name", "Name", r++));
//		bsg.attrs.add(new ResultAttribute("Caption", "Caption", r++));
//		bsg.attrs.add(new ResultAttribute("SAP_CreationTime", "CreatedAt", r++));
//		bsg.attrs.add(new ResultAttribute("SAP_LastChangedBy", "ChangedBy", r++));
//		bsg.attrs.add(new ResultAttribute("SAP_LastModificationTime", "ModifiedAt", r++));
//		es.add(bsg);
//
//		PiEntity bst = new PiEntity(this, -1, sld, "SAP_BusinessSystemPath", "Business System Path", q++);
//		bst.ok = true;
//		r = 0;
//		bst.attrs.add(new ResultAttribute("Name", "Name", r++));
//		bst.attrs.add(new ResultAttribute("Caption", "Caption", r++));
//		bst.attrs.add(new ResultAttribute("SAP_CreationTime", "CreatedAt", r++));
//		bst.attrs.add(new ResultAttribute("SAP_LastChangedBy", "ChangedBy", r++));
//		bst.attrs.add(new ResultAttribute("SAP_LastModificationTime", "ModifiedAt", r++));
//		es.add(bst);
		
		return false;
	}
	/**
	 * Собирает для repository и directory
	 * @param side		для какой стороны составлять индекс
	 * @param thrcnt	число тредов
	 */
	protected List<PiEntity> getMetaOnline(Side side) throws IOException, InterruptedException, SAXException, ExecutionException {
		assert side!=null;
		// всё скачать и построить карту
		List<PiEntity> es = new ArrayList<PiEntity>(200);
		if (side == Side.SLD) return es;
		final URL u = side.url(uroot);
		final HTask hDocs = new HTask("refreshMetaIdx", establishGET(u, true));
		List<FutureTask<HTask>> queue = new LinkedList<FutureTask<HTask>>();
		FutureTask<HTask> t = HUtil.addHTask(hDocs);
		t.get();
		
		if (hDocs.ok) {
			SQEntityAttr sq = PiEntity.parse_ra(hDocs.bis, "types");
			 
			for (int i=0; i<sq.size; i++) {
				PiEntity e = new PiEntity(this, 0, side, sq.matrix[i][0], sq.matrix[i][1], i); 
				es.add(e);
				if (log.isLoggable(Level.FINEST)) log.finest("collectDocsRA: extracted " + e.intname);
				HTask h = e.collectRA(this);
				h.refObj = e;
				queue.add(HUtil.addHTask(h));
			}

			for (FutureTask<HTask> x: queue) {
				HTask h = x.get();
				assert h.rc == 200 : "Non-successfull http rc: " + h.rc + " for OK HTask"; 
				assert h.bis!=null : "Empty bis for " + h.hc.getURL().toExternalForm() + " " + h.method + " " + h.post;
				PiEntity e = (PiEntity)h.refObj;
				assert e!=null;
				sq = PiEntity.parse_ra(h.bis, "result");
				e.addRA(sq);
				queue.remove(h);
			}
		}
		return es;
	}
	protected void addEntity(PiEntity e) {
		assert e.side!=null && e.intname!=null && !e.intname.isEmpty() : "tried to add invalid entity";
		entities.put(e.toString(), e);
		log.config("configured entity " + e);
	}
	public PiEntity getEntity(Side side, String intname) {
		assert side!=null && intname!=null;
		String k = side.txt()+"|"+intname;
		return entities.get(k);
	}

//	private boolean ping_service(URL u, boolean get, boolean auth, String failed) {
//		try {
//			HttpURLConnection h = get ? establishGET(u, auth) : establishPOST(u, auth);
//			h.connect();
//			if (h.getResponseCode()!=HttpURLConnection.HTTP_OK)
//				log.severe(DUtil.format(failed,u.toExternalForm(),h.getResponseCode(),h.getResponseMessage()));
//			else
//				return true;
//		} catch (Exception e) { 
//			log.log(Level.SEVERE, "HTTP access error to " + u.toExternalForm(), e);
//			log.warning(DUtil.format(failed,u.toExternalForm(),e.getMessage(),"" ) );
//		}
//		return false;
//	}
	public boolean ping(boolean cpa) {
		boolean b = cpa;
		// Test ROOT without authorization
//		b = b && ping_service(uroot, true, false, "ping_failed");
//		b = b && ping_service(urep, false, true, "rep_unaccessible");
//		b = b && ping_service(urep, false, true, "dir_unaccessible");
//		b = b && ping_service(uaf_schxml, true, false, "afschxml_unaccessible");
//		if (b && cpa) try {
//			cpacache = askCpaCache();
//		} catch (Exception e) {
//			log.severe(DUtil.format("CPA_unaccessible",ucpahtml.toExternalForm(),e.getMessage(),""));
//			b = false;
//		}
//		assert !b || !cpa || (cpacache!=null) : "cpacache not assigned for successful ping";
		return b;
	}
	/**
	 * Новый интерфейс, проверяет доступность сервиса 
	 * @param side
	 * @return
	 */
	public boolean isSideAvailable(Side side) {
		URL u = uroot;
		try {
			u = side.url(uroot);
			HttpURLConnection h = establishGET(u, true);
			h.connect();
			return (h.getResponseCode()==HttpURLConnection.HTTP_OK);
		} catch (Exception e) { 
			log.log(Level.SEVERE, "HTTP access error to " + u + " side " + side, e);
		}
		return false;
	}


	public boolean register(String action, Object...objs) {
		if (this.action==null) {
			this.action = action;
			this.actobjs = objs;
			return true;
		} else
			return false;
	}
	
	public List<PiObject> askIndexOnline(PiEntity e, boolean del) throws IOException, SAXException, InterruptedException, ExecutionException {
		assert e!=null : "Entity must be present";
		
//		String qdel = null, qactiv = null, s3 = "result=RA_XILINK&result=OBJECTID&result=VERSIONID";
		if (log.isLoggable(Level.CONFIG)) 
			log.config("askIndex started for entity " + e.intname + " deleted=" + del);

		List<PiObject> rez = new LinkedList<PiObject>();
		HTask ha = e.makeOnlineHTask(this, del);
		FutureTask<HTask> fa = HUtil.addHTask(ha);

		ha = fa.get();
		SimpleQueryHandler sqh = PiEntity.handleStream(ha.bis);
		e.parse_index(rez, sqh, del);
		return rez;
	}

//	private CPACache askCpaCache() throws IOException {
//		HttpURLConnection h = establishPOST(ucpahtml, true);
//		DUtil.putPOST(h, "display=Display+CPA+Cache+Content");
//		h.connect();
//		Document d = Jsoup.parse(h.getInputStream(), C_ENC, "");
//		CPACache c = new CPACache();
//		c.parseHtml(d.select("html body form pre").text());
//		return null; //TODO: c;
//	}
//	private Object askAfSch() throws IOException {
//		HttpURLConnection h = establishGET(uaf_schxml, false);
//		h.connect();
//		Document d = Jsoup.parse(h.getInputStream(), C_ENC, "");	//XXX
//		assert d!=null;
//		Elements es = d.select("scheduler");
//		assert es!=null && es.size() > 0 : d.toString();
//		return null; // TODO:
//	}
	
//	public boolean askCcStatus(byte[] oid) throws MalformedURLException, IOException {
//		URL u = Side.askCcStatus(uroot, "status", oid);
//		HttpURLConnection h = establishGET(u, true);
//		boolean b = false;
//		assert cpacache!=null : "CPA cache is not initialized";
//		
//		askAfSch();
//		if ("1" != "9") return true;
//	
//		if (h.getResponseCode() == HttpURLConnection.HTTP_OK) {
////			d = Jsoup.parse(h.getInputStream(), "UTF-8", ""); // XXX
////			Elements es = d.select("ChannelStatusResult Channels Channel");
////			assert es!=null && es.size() > 0 : u.toExternalForm() + "\n" + d.toString() ;
////			if (es==null || es.size()==0) System.err.println("ZZZZZZZZ"+d.toString());
////			String as = e.select("ActivationState").text();
////			String s = e.select("ChannelState").text();
////			System.out.println("ActivationState: " + as + " ChannelState: " + s);
//			b = true;
//		} else if (h.getResponseCode() == HttpURLConnection.HTTP_INTERNAL_ERROR) {
////			d = Jsoup.parse(h.getErrorStream(), "UTF-8", ""); // XXX
////			String exc = d.select("ErrorInformation Exception").text();
////			String desc = d.select("ErrorInformation Description").text();
////			System.err.println(exc + ": " + desc);
//		} else {
//			throw new RuntimeException("unexpected http response: " + h.getResponseCode() + " when get " + u.toExternalForm());
//		}
//		return b;
//	}
	public void addObject(PiObject o, long session_id) throws SQLException {
		// TODO: написать тексты для ассертов
		assert hostdb != null && !hostdb.isClosed() && !hostdb.isReadOnly();
		if (psInboxDel==null) psInboxDel = DUtil.prepareStatement(hostdb, "hosql_inbox_del");
		if (psInboxIns==null) psInboxIns = DUtil.prepareStatement(hostdb, "hosql_inbox_ins");
		
		
//		hosql_insOV_dirty=INSERT INTO inbox (object_ref,object_id,version_id,session_id,url) \
//	    VALUES (?1,?2,?3,?4,?5);
		assert o!=null;
		assert session_id!=0 && session_id!=-1;
		assert o.refDB!=0;
		assert o.objectid!=null;
		assert o.versionid!=null;
		assert o.qryref!=null;
		assert !o.deleted : "tried to add deleted object " + o;
		DUtil.setStatementParams(psInboxIns, o.refDB, o.objectid, o.versionid, session_id, o.qryref);
		DUtil.setStatementParams(psInboxDel, o.objectid, o.versionid);
		psInboxDel.addBatch();
		psInboxIns.addBatch();
	}
	
	public void commitObject() throws SQLException {
		lock.lock();
		try {
			if (psInboxDel!=null) psInboxDel.executeBatch();
			if (psInboxIns!=null) psInboxIns.executeBatch();
			hostdb.commit();
        } finally {
            lock.unlock();
        }
	}
	public void executeUpdate(boolean commit, PreparedStatement ... ps) throws SQLException {
		lock.lock();
		try {
			for (PreparedStatement p: ps) p.executeUpdate();
			if (commit) hostdb.commit();
        } finally {
            lock.unlock();
        }
	}
//	int threadcount = 0;
//	ArrayList<PayloadFetcher> alPF = null;
//
//	class PayloadFetcher implements Runnable {
//		URL u;
//		long num;
//		int rc = -1;
//		HttpURLConnection h = null;
//		byte[] vid;
//		boolean error = false, ok = false;
//		PayloadFetcher (String u, long lq, byte[] ver) throws MalformedURLException {
//			this.u = new URL(u);
//			num = lq;
//			vid = ver;
//		}
//		public void run() {
//			error = false;
//			ok = false;
//			try {
//				h = establishGET(u, true);
//				h.connect();
//				rc = h.getResponseCode();
//				if (rc != HttpURLConnection.HTTP_OK) {
//					log.severe("HTTP client error " + h.getResponseCode() + " " + h.getURL());
//					h.disconnect();
//					error = true;
//					System.out.println("ERROR " + num + " " + u.toExternalForm());
//				} else {
//					ok = true;
//					System.out.println("OK " + num + " " + u.toExternalForm());
//				}
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				System.err.println("Error when " + num + ", " + u.toExternalForm() );
//				e.printStackTrace();
//				Thread.currentThread().interrupt();
//			}
//			threadcount--;
//		}
//		byte[] readBytes() throws IOException {
//			System.out.println(num);
//			ByteArrayOutputStream baos = new ByteArrayOutputStream(500000);
//			GZIPOutputStream z = new GZIPOutputStream(baos);
//			InputStream in = h.getInputStream();
//			try {
//				int i = in.read();
//				while (i!=-1) {
//					z.write(i);
//					i = in.read();
//				}
//			} catch (IOException i) {
//				return null;
//			}
//			in.close();
//			h.disconnect();
//			z.finish();
//			z.flush();
//			ok = false;
//			return baos.toByteArray();
//		}
//	}

	protected boolean download() throws SQLException, MalformedURLException, IOException {
		int zzz = 0/0;
		
		
		assert hostdb!=null && !hostdb.isClosed() && !hostdb.isReadOnly() : "host DB error";
		//hosql_inbox_unk=SELECT inbox_id,url,dlcount FROM inbox;
		//hosql_inbox_unks=SELECT object_ref,object_id,version_id,session_id,url,dlcount FROM inbox WHERE inbox_id=?1;
		//hosql_objlink_ins=INSERT INTO objlink (object_ref,object_id,version_id,session_id,url,bloz)
		PreparedStatement psUnk = DUtil.prepareStatement(hostdb, "hosql_inbox_unk")
				, psUnks = DUtil.prepareStatement(hostdb, "hosql_inbox_unks")
				, psUnkd = DUtil.prepareStatement(hostdb, "hosql_inbox_unkd")
				, psOLP = DUtil.prepareStatement(hostdb, "hosql_objlink_ins")
				;
		/*
		ResultSet rs = psUnk.executeQuery();
		HashMap<Integer,Long> hm = new HashMap<Integer,Long>(100);
		int la;
		long inbox_id;
		while (rs.next()) {
			inbox_id = rs.getLong(1); //, dlcount = rs.getLong(3);
			String url = rs.getString(2);
			System.out.println(url);
			la = HUtil.addGet(establishGET(new URL(url), true));
			hm.put(la, inbox_id);
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream(500000);
		GZIPOutputStream z;
		
		while (hm.size()>0) for (Integer x: hm.keySet()) if (HUtil.isDone(x)) {
			inbox_id = hm.get(x);
			ByteArrayInputStream bis = HUtil.getBAIS(x);
			baos.reset();
			z = new GZIPOutputStream(baos);
			int i = bis.read();
			while (i!=-1) {
				z.write(i);
				i = bis.read();
			}
			z.finish();
			z.flush();
			rs = DUtil.setStatementParams(psUnks, inbox_id).executeQuery();
			if (rs.next()) {
				DUtil.setStatementParams(psOLP,rs.getLong(1),rs.getBytes(2),rs.getBytes(3),rs.getLong(4),rs.getString(5),baos.toByteArray());
				i = psOLP.executeUpdate();
				assert i==1;
				DUtil.setStatementParams(psUnkd, inbox_id);
				executeUpdate(true, psUnkd);
			} else {
				int j = 0/0; 
			}
			hm.remove(x);
			break;
		}
		*/
		return true;
	}

	protected ResultSet getPayload(long ref, byte[] version_id) throws SQLException {
		return DUtil.setStatementParams(psGP, ref, version_id).executeQuery();
		
	}
	public static boolean createHostDB(Connection hostcon) throws SQLException {
		for (String s: DUtil.sqlKeySet) if (s.startsWith("hosql_init")) {
			log.config(s);
			DUtil.prepareStatement(hostcon, s).executeUpdate();
			hostcon.commit();
		}
		return true;
	}
	public boolean migrateHostDB(String newDbFile) throws SQLException {
		assert !newDbFile.contains("./\\") : "dot or slashes in temp db aren't supported for attach yet";
		Connection newcon = DriverManager.getConnection("jdbc:sqlite:" + newDbFile);
		newcon.setAutoCommit(false);
		createHostDB(newcon);
		newcon.close();

		//IMPORTANT!!! главная база (куда аттачим) должна быть в автокоммите
		//see http://stackoverflow.com/a/9119346/521359
		//for DDL statement only it's required
		hostdb.setAutoCommit(true);
		PreparedStatement ps = DUtil.prepareStatement(hostdb, "hosql_migr_01_attach", newDbFile);
		ps.executeUpdate();
		hostdb.setAutoCommit(false);
		// все DML-операции уже могут быть коммичены явно
		ps = DUtil.prepareStatement(hostdb, "hosql_migr_02_clear");
		ps.executeUpdate();
		hostdb.commit();
		ps = DUtil.prepareStatement(hostdb, "hosql_migr_03_objlink");
		ps.executeUpdate();
		hostdb.commit();
		return true;
	}

	// --------------------------------------------------------------------
	public List<SWCV> __getSwcvOnline(PiEntity eswcv) throws IOException, SAXException, ParseException, InterruptedException, ExecutionException {
		assert eswcv != null: "SWCV isn't given as the entity";
		List<SWCV> def = new ArrayList<SWCV>(30), deps = new ArrayList<SWCV>(100);

		HTask h = new HTask("SWCVdef", establishPOST(Side.Repository.url(uroot), true), SWCV.qdef);
		FutureTask<HTask> t = HUtil.addHTask(h);

		HTask hd = new HTask("SWCVdep", establishPOST(Side.Repository.url(uroot), true), SWCV.qdeps);
		FutureTask<HTask> td = HUtil.addHTask(hd);

		h = t.get();
		assert h.ok : "SWCVdef extraction problem";
		eswcv.parse_index(def, PiEntity.handleStream(h.bis));

		hd = td.get();
		assert hd.ok : "SWCVdeps extraction problem";
		assert hd.bis!=null : "SWCV definition stream is null";
		eswcv.parse_index(deps, PiEntity.handleStream(hd.bis));

		log.config("There is " + def.size() + " swcv and " + deps.size() + " dependencies");
		for (SWCV sw: def) 
			sw.setDeps(deps);
		assert deps.size()==0 : "There are " + deps.size() + " unhandled dependencies yet";
		return def;
	}

}
