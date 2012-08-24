package com.pinternals.diffo;

import java.io.ByteArrayOutputStream;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.xml.sax.SAXException;

import sun.misc.BASE64Encoder;

import com.pinternals.diffo.PiEntity.SQEntityAttr;

public class PiHost implements Runnable {
	private static Logger log = Logger.getLogger(Diffo.class.getName());
	private static int MAX_RETR_ATTEMPTS = 100;

	/** Для каждого хоста может быть свой прокси, поэтому пусть будет здесь */
	public String basicAuth=null, uname=null, passwd=null;
	private String action = null;
	private Object actobjs = null;
	public String sid = null;
	public Long host_id = new Long(-1);
//	private Diffo diffo = null;
	protected Connection hostdb = null;
	String hostdbfn = null; 
	private PreparedStatement psNewOV = null, psGP = null; 

	public URL uroot;
	private URL ucpahtml, uaf_schxml;
	private CPACache cpacache = null;
	private int timeoutMillis=30000;
	private Proxy proxy = null;
	private int dirtycnt = 0;

	public HashMap<String,PiEntity> entities = new HashMap<String,PiEntity>(20);
//	public boolean ime_sapcom = false, ime_customer = false;

	public HashMap<Long,SWCV> swcv = null;

	public PiHost (Diffo d, String sid, String sURL, long host_id, Connection hostdb) 
	throws MalformedURLException, SQLException {
		this.proxy = d.proxy;
		this.sid = sid;
//		this.diffo = d;
		this.uroot = new URL(sURL);
//		this.ucpahtml = new URL(sURL + Side.C_CPA_S);
//		this.uaf_schxml = new URL(sURL + Side.C_AF_SCH_XML);
		this.host_id = host_id;
		this.hostdb = hostdb;
		this.hostdbfn = "diffo.DPI.6.db";
		psNewOV = DUtil.prepareStatement(hostdb, "hosql_insOV_dirty");
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
	private HttpURLConnection establishPOST(URL u, boolean useCredentials) throws MalformedURLException, IOException {
		HttpURLConnection huc = DUtil.getHttpConnection(proxy, u, timeoutMillis);
		huc.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
		huc.setRequestMethod("POST");
		if (useCredentials) huc.setRequestProperty("Authorization", basicAuth);
		return huc;
	}
	private HttpURLConnection establishGET(URL u, boolean useCredentials) 
	throws IOException {
		HttpURLConnection huc = DUtil.getHttpConnection(proxy, u, timeoutMillis);
		huc.setRequestMethod("GET");
		if (useCredentials) huc.setRequestProperty("Authorization", basicAuth);
		return huc;
	}
	
	// --------------------------------------------------------- XI services
	private boolean buildSLD (Side sld, ArrayList<PiEntity> es) {
		int q=0;
		PiEntity bs = new PiEntity(-1, sld, "SAP_BusinessSystem", "Business System", q++);
		int r = 0;
		bs.attrs.add(new ResultAttribute("Name", "Name", r++));
		bs.attrs.add(new ResultAttribute("Caption", "Caption", r++));
		bs.attrs.add(new ResultAttribute("SAP_CreationTime", "CreatedAt", r++));
		bs.attrs.add(new ResultAttribute("SAP_LastChangedBy", "ChangedBy", r++));
		bs.attrs.add(new ResultAttribute("SAP_LastModificationTime", "ModifiedAt", r++));
		bs.attrs.add(new ResultAttribute("SAP_GlobalUniqueID", "GUID", r++));
		es.add(bs);
		
		PiEntity ag = new PiEntity(-1, sld, "SAP_BusinessSystemGuid", "SAP_BusinessSystemGuid", q++);
		r=0;
		ag.attrs.add(new ResultAttribute("SAP_CreationTime", "CreatedAt", r++));
		ag.attrs.add(new ResultAttribute("SAP_LastChangedBy", "ChangedBy", r++));
		ag.attrs.add(new ResultAttribute("SAP_LastModificationTime", "ModifiedAt", r++));
		ag.attrs.add(new ResultAttribute("SAP_GlobalUniqueID", "SAP_GlobalUniqueID", r++));
		ag.attrs.add(new ResultAttribute("SAP_LogicalBusinessSystem", "SAP_LogicalBusinessSystem", r++));
		es.add(ag);
		
		PiEntity bsg = new PiEntity(-1, sld, "SAP_BusinessSystemGroup", "Business System Group", q++);
		r = 0;
		bsg.attrs.add(new ResultAttribute("Name", "Name", r++));
		bsg.attrs.add(new ResultAttribute("Caption", "Caption", r++));
		bsg.attrs.add(new ResultAttribute("SAP_CreationTime", "CreatedAt", r++));
		bsg.attrs.add(new ResultAttribute("SAP_LastChangedBy", "ChangedBy", r++));
		bsg.attrs.add(new ResultAttribute("SAP_LastModificationTime", "ModifiedAt", r++));
		es.add(bsg);

		PiEntity bst = new PiEntity(-1, sld, "SAP_BusinessSystemPath", "Business System Path", q++);
		r = 0;
		bst.attrs.add(new ResultAttribute("Name", "Name", r++));
		bst.attrs.add(new ResultAttribute("Caption", "Caption", r++));
		bst.attrs.add(new ResultAttribute("SAP_CreationTime", "CreatedAt", r++));
		bst.attrs.add(new ResultAttribute("SAP_LastChangedBy", "ChangedBy", r++));
		bst.attrs.add(new ResultAttribute("SAP_LastModificationTime", "ModifiedAt", r++));
		es.add(bst);
		return true;
	}
	/**
	 * Собирает для repository и directory
	 */
	protected ArrayList<PiEntity> collectDocsRA(Side side) throws IOException, SAXException {
		// всё скачать и построить карту
		ArrayList<PiEntity> es = new ArrayList<PiEntity>(200);
		if (side == Side.SLD) {
			buildSLD(side, es);
			return es;
		}
		URL u = side.url(uroot);
		HttpURLConnection h = establishGET(u, true);
		h.connect();
		SQEntityAttr sq = PiEntity.parse_ra(h.getInputStream(), "types");
		h.disconnect();
		for (int i=0; i<sq.size; i++) 
			es.add(new PiEntity(0,side,sq.matrix[i][0],sq.matrix[i][1],i));

		for (PiEntity e: es) {
			h = establishPOST(u, true);
			DUtil.putPOST(h, e.side.createQueryResultAttributes(e.intname));
			h.connect();
			sq = PiEntity.parse_ra(h.getInputStream(), "result");
			h.disconnect();
			for (int j=0; j<sq.size; j++)
				e.attrs.add(new ResultAttribute(sq.matrix[j][0], sq.matrix[j][1], j));
		}
		return es;
	}
	protected void addEntity(PiEntity e) {
		assert e.side!=null && e.intname!=null && !e.intname.isEmpty() : "tried to add invalid entity";
		entities.put(e.side.txt()+"|"+e.intname, e);
	}
	public PiEntity getEntity(Side side, String intname) {
		return entities.get(side.txt()+"|"+intname);
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
			if (h.getResponseCode()!=HttpURLConnection.HTTP_OK)
				return false;
			else
				return true;
		} catch (Exception e) { 
			log.log(Level.SEVERE, "HTTP access error to " + u + " side " + side, e);
		}
		return false;
	}

	/**
	 * Работает неверно из-за зависимостей в SWCV. Не трогать это старое, см. askSwcvSeparated
	 * @return
	 * @throws IOException
	 * @throws SAXException
	 * @throws ParseException
	 */
	public ArrayList<SWCV> askSWCV() throws IOException, SAXException, ParseException {
		PiEntity e = getEntity(Side.Repository, "workspace");
		assert e != null: "SWCV not found in entities";
		
		assert false: "Code is not for using";
		int i=0/0;
		
		String s = "qc=All+software+components&syncTabL=true&deletedL=N&xmlReleaseL=7.1&queryRequestXMLL=&types=workspace&" + e.getQueryPrepareParse() + "&action=Start+query"; 
		HttpURLConnection h = establishPOST(Side.Repository.url(uroot), true);
		DUtil.putPOST(h, s);
		h.connect();
		ArrayList<PiObject> tmp = e.parse_index(h.getInputStream(),false);
		if (tmp.size()==0) {
			log.warning(DUtil.format("swcv_retr_failure", sid));
			return null;
		}
		ArrayList<SWCV> rez = new ArrayList<SWCV>(tmp.size()); 
		for (PiObject po: tmp) 
			rez.add(new SWCV(po));
		return rez;
	}
	
	public ArrayList<SWCV> askSwcvSeparated() throws IOException, SAXException, ParseException {
		PiEntity e = getEntity(Side.Repository, "workspace");
		assert e != null: "SWCV not found in entities";
		String sDef, sDep;
		// definitions
		sDef="qc=All+software+components&syncTabL=true&deletedL=B&xmlReleaseL=7.1&queryRequestXMLL=&types=workspace&result=COMPONENT_NAME&result=COMPONENT_VENDOR&result=WS_ID&result=MODIFYDATE&result=MODIFYUSER&result=WS_NAME&result=CAPTION&result=ELEMENTTYPEID&result=NAME&result=VENDOR&result=VERSION&result=SWC_GUID&result=WS_TYPE&result=EDITABLE&result=ORIGINAL&result=ELEMENTTYPEID&result=DEVLINE&result=WS_ORDER&action=Start+query";
		// dependencies
		sDep="qc=All+software+components&syncTabL=true&deletedL=B&xmlReleaseL=7.1&queryRequestXMLL=&types=workspace&result=DEPTYPE&result=WS_ID&result=DEPWS_ID&result=DEPWS_NAME&result=WS_ORDER&result=SEQNO&result=DEVLINE&action=Start+query";
		// whole template
		HttpURLConnection h = establishPOST(Side.Repository.url(uroot), true);
		DUtil.putPOST(h, sDef);
		h.connect();
		ArrayList<PiObject> tmp = e.parse_index(h.getInputStream(),false), tmp2;
		h.disconnect();
		if (tmp.size()==0) {
			log.warning(DUtil.format("swcv_retr_failure", sid));
			return null;
		}
		// get dependencies
		h = establishPOST(Side.Repository.url(uroot), true);
		DUtil.putPOST(h, sDep);
		h.connect();
		tmp2 = e.parse_index(h.getInputStream(),false);
		h.disconnect();
		if (tmp2.size()==0) {
			log.warning(DUtil.format("swcv_retr_failure", sid));
			return null;
		}
		log.config("There is " + tmp.size() + " swcv and " + tmp2.size() + " dependencies");
		ArrayList<SWCV> rez = new ArrayList<SWCV>(tmp.size()); 
		for (PiObject po: tmp) {
			SWCV sw = new SWCV(po);
			sw.addDep(tmp2,true);
			rez.add(sw);
		}
		return rez;
	}
	
	
	public boolean register(String action, Object...objs) {
		if (this.action==null) {
			this.action = action;
			this.actobjs = objs;
			return true;
		} else
			return false;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		if (action==null) return;
		if ("askIndex".equals(action)) {
			
		}
	}
	
	public ArrayList<PiObject> askIndex(PiEntity e) throws IOException, SAXException {
		assert e!=null : "Entity must be present";
		String qdel = null, qactiv = null;
		if (log.isLoggable(Level.CONFIG)) 
			log.config("askIndex started for entity " + e.intname);

		if (e.side.equals(Side.Repository)){
			qactiv = "qc=All+software+components&syncTabL=true&deletedL=N&xmlReleaseL=7.1&queryRequestXMLL=&types=" + e.intname + "&" + e.getQueryPrepareParse() + "&action=Start+query"; 
			qdel = "qc=All+software+components&syncTabL=true&deletedL=D&xmlReleaseL=7.1&queryRequestXMLL=&types=" + e.intname + "&" + e.getQueryPrepareParse() + "&action=Start+query"; 
		} else if (e.side.equals(Side.Directory)){
			qactiv = "qc=Default+%28for+directory+objects%29&syncTabL=true&deletedL=N&xmlReleaseL=7.1&types=" + e.intname + "&" + e.getQueryPrepareParse() + "&action=Start+query";
			qdel = "qc=Default+%28for+directory+objects%29&syncTabL=true&deletedL=D&xmlReleaseL=7.1&types=" + e.intname + "&" + e.getQueryPrepareParse() + "&action=Start+query";
		} else if (e.side == Side.SLD) {
			return SLD.getObjects(e, Side.SLD.url(uroot), uname, passwd);
		} else
			assert false: "Unknown side: "+e.side.txt();

		int ia=0, id=0;
		ArrayList<PiObject> rez = null, del = null ;
		if (qactiv!=null) {
			HttpURLConnection ha = establishPOST(e.side.url(uroot), true);
			DUtil.putPOST(ha, qactiv);
			ha.connect();
			rez = e.parse_index(ha.getInputStream(),false);
			ia = rez.size();
			ha.disconnect();
			ha = null;
		}
		if (qdel!=null) {
			HttpURLConnection hd = establishPOST(e.side.url(uroot), true);
			DUtil.putPOST(hd, qdel);
			hd.connect();
			del = e.parse_index(hd.getInputStream(),true);
			hd.disconnect();
			if (del.size()>0) {
				if (rez==null) 
					rez = del;
				else
					rez.addAll(del);
			}
			id = rez.size();
			del = null;
			hd = null;
		}
		log.info(DUtil.format("sq01", e.intname, e.entity_id, ia, id));
		return rez;
	}
	private CPACache askCpaCache() throws IOException {
		HttpURLConnection h = establishPOST(ucpahtml, true);
		DUtil.putPOST(h, "display=Display+CPA+Cache+Content");
		h.connect();
//		Document d = Jsoup.parse(h.getInputStream(), C_ENC, "");
//		CPACache c = new CPACache();
//		c.parseHtml(d.select("html body form pre").text());
		return null; //TODO: c;
	}
	private Object askAfSch() throws IOException {
		HttpURLConnection h = establishGET(uaf_schxml, false);
		h.connect();
//		Document d = Jsoup.parse(h.getInputStream(), C_ENC, "");	//XXX
//		assert d!=null;
//		Elements es = d.select("scheduler");
//		assert es!=null && es.size() > 0 : d.toString();
		return null; // TODO:
	}
	
	public boolean askCcStatus(byte[] oid) throws MalformedURLException, IOException {
		URL u = Side.askCcStatus(uroot, "status", oid);
		HttpURLConnection h = establishGET(u, true);
		boolean b = false;
		assert cpacache!=null : "CPA cache is not initialized";
		
		askAfSch();
		if ("1" != "9") return true;
	
		if (h.getResponseCode() == HttpURLConnection.HTTP_OK) {
//			d = Jsoup.parse(h.getInputStream(), "UTF-8", ""); // XXX
//			Elements es = d.select("ChannelStatusResult Channels Channel");
//			assert es!=null && es.size() > 0 : u.toExternalForm() + "\n" + d.toString() ;
//			if (es==null || es.size()==0) System.err.println("ZZZZZZZZ"+d.toString());
//			String as = e.select("ActivationState").text();
//			String s = e.select("ChannelState").text();
//			System.out.println("ActivationState: " + as + " ChannelState: " + s);
			b = true;
		} else if (h.getResponseCode() == HttpURLConnection.HTTP_INTERNAL_ERROR) {
//			d = Jsoup.parse(h.getErrorStream(), "UTF-8", ""); // XXX
//			String exc = d.select("ErrorInformation Exception").text();
//			String desc = d.select("ErrorInformation Description").text();
//			System.err.println(exc + ": " + desc);
		} else {
			throw new RuntimeException("unexpected http response: " + h.getResponseCode() + " when get " + u.toExternalForm());
		}
		return b;
	}
	public void addObject(PiObject o, long session_id, boolean batch) throws SQLException {
		// TODO: написать тексты для ассертов
		assert hostdb != null && !hostdb.isClosed() && !hostdb.isReadOnly();
		assert psNewOV != null;
//		hosql_insOV_dirty=INSERT INTO objlink (object_ref,object_id,version_id,session_id,url,is_dirty) \
//	    VALUES (?1,?2,?3,?4,?5,1);
		assert o!=null;
		assert session_id!=0 && session_id!=-1;
		assert o.refDB!=0;
		assert o.objectid!=null;
		assert o.versionid!=null;
		assert o.rawref!=null;
		assert !o.deleted : "tried to add deleted object " + o;
		DUtil.setStatementParams(psNewOV, o.refDB, o.objectid, o.versionid, session_id, o.rawref);
		if (log.isLoggable(Level.FINE))
			log.fine("before psNewOV for oid/vid " + UUtil.getStringUUIDfromBytes(o.objectid) + "/" + UUtil.getStringUUIDfromBytes(o.versionid));
		if (batch)
			psNewOV.addBatch();
		else
			psNewOV.executeUpdate();
		dirtycnt++;
	}

	protected void addObjectCommit(boolean batch) throws SQLException {
		assert hostdb != null && !hostdb.isClosed() && !hostdb.isReadOnly();
		assert psNewOV != null;
		if (batch) psNewOV.executeBatch();
		hostdb.commit();
	}

	protected boolean download(boolean check) throws SQLException, MalformedURLException, IOException {
		assert hostdb!=null && !hostdb.isClosed() && !hostdb.isReadOnly() : "host DB error";
		
		PreparedStatement psPut = DUtil.prepareStatement(hostdb, "hosql_undirty")
				,psInc = DUtil.prepareStatement(hostdb, "hosql_incdirty") ;
		assert psPut != null : "host DB error when prepare update clause";
		ResultSet rs = DUtil.prepareStatement(hostdb, "hosql_getdirty").executeQuery();
		int q=0, errors=0, ok=0, nonretr=0;
		ByteArrayOutputStream baos = new ByteArrayOutputStream(500000);
		while (rs.next()) {
			long is_dirty = rs.getLong(4);
			assert is_dirty!=0 : "Attempt to retrieve already retrieved (is_dirty=0)";
			if (is_dirty >= MAX_RETR_ATTEMPTS) {
				nonretr++;
				continue;
			}
			URL u = new URL(rs.getString(1));
			long ref = rs.getLong(2);
			byte []vid = rs.getBytes(3);
			HttpURLConnection h = establishGET(u, true);
			h.connect();
			if (h.getResponseCode()!=HttpURLConnection.HTTP_OK) {
				log.severe("HTTP client error " + h.getResponseCode() + " " + h.getURL());
				h.disconnect();
				// увеличиваем счётчик ошибок
				DUtil.setStatementParams(psInc, ref, vid);
				psInc.addBatch();
				errors++;
			} else {
				baos.reset();
				GZIPOutputStream gos = new GZIPOutputStream(baos);
				int i = h.getInputStream().read(), z=0;
				while (i!=-1) {
					gos.write(i);
					z++;
					i = h.getInputStream().read();
				}
				h.getInputStream().close();
				h.disconnect();
				gos.close();
				baos.close();
				byte ba[] = baos.toByteArray();
				log.config(DUtil.format("piobject_download", u.toExternalForm(), z, ba.length));
				DUtil.setStatementParams(psPut, ref, vid, ba);
				psPut.addBatch();
				ok++;
			}
			q++;
		}
		if (errors>0) {
			psInc.executeBatch();
			hostdb.commit();
		}
		if (ok>0) {
			psPut.executeBatch();
			hostdb.commit();
		}

		assert !check || q==dirtycnt : 
			"dirty URLs counts are mismatch: now it is " + q + " but registered before " + dirtycnt;

		if (q==0)
			log.config(DUtil.format("piobject_download_empty",this.uroot.toExternalForm()));
		else if (errors==0)
			log.config(DUtil.format("piobject_download_ok",this.uroot.toExternalForm(), q));
		else 
			log.severe(DUtil.format("piobject_download_error",this.uroot.toExternalForm(), q, errors));

		return q==0 || errors==0;
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
}
