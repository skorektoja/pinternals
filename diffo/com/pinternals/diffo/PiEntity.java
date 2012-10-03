package com.pinternals.diffo;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.FutureTask;

import org.ccil.cowan.tagsoup.Parser;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


public class PiEntity {
	class ResultAttribute {
		String internal, caption;
		int seqno;
		public ResultAttribute (String internal, String caption, int seqno) {
			this.internal = internal;
			this.caption = caption;
			this.seqno = seqno;
		}
	}	
	public String intname, title;
	public long entity_id, seqno;
	public Side side = null;
	public ArrayList<ResultAttribute> attrs = new ArrayList<ResultAttribute>(20);
	public boolean ok = false;
	protected PiHost host = null;
	private Long lastDtFrom = null;
	protected long affected = 0, minDT = 0;
	
	protected PiEntity (PiHost p, long entity_id, Side side, String intname, String title, int seqno) {
		this.intname = intname;
		this.title = title;
		this.side = side;
		this.seqno = seqno;
		this.entity_id = entity_id;
		this.host = p;
	}
	protected void setLastInfo(Long minDT1, Long affected, String session_close_dt) {
		lastDtFrom = minDT1;
		if (minDT1!=null)
			System.out.println("minDT=" + lastDtFrom + " affected=" + affected + " session_close_dt=" + session_close_dt);
	}
	synchronized protected void incAffected() {
		affected++;
	}
	protected void addAttr(String intname, String caption, int seqno) {
		ResultAttribute ra = new ResultAttribute(intname,caption, seqno);
		attrs.add(ra);
	}
	protected void addRA(SQEntityAttr sq) {
		for (int i=0; i<sq.size; i++) {
			ResultAttribute ra = new ResultAttribute(sq.matrix[i][0], sq.matrix[i][1], i); 
			attrs.add(ra);
		}
	}
	public String toString() {
		return side + "|" + intname;
	}

	public static SQEntityAttr parse_ra(InputStream is, String n) throws IOException, SAXException {
		assert is!=null && n!=null : "PiEntity.parse_ra input checks assert";
		Parser p = new Parser();
		SQEntityAttr sq = new SQEntityAttr(n);
		p.setContentHandler(sq);
		p.parse(new InputSource(is));
		is.close();
		return sq;
	}	
	protected HTask collectRA(PiHost p) throws MalformedURLException, IOException {
		String queryDir = "qc=Default+(for+directory+objects)&syncTabL=true&deletedL=B&xmlReleaseL=7.1&types=" + intname + "&action=Refresh+depended+values";
		String queryRep = "qc=All+software+components&syncTabL=true&deletedL=B&xmlReleaseL=7.1&queryRequestXMLL=&types=" + intname + "&action=Refresh+depended+values";

		HTask h = new HTask("CollectRA("+intname+")", 
				p.establishPOST(side.url(p.uroot), true), 
				side == Side.Repository ? queryRep : 
					side == Side.Directory? queryDir : 
						(new RuntimeException("PiEntity: unknown side. Not implemented yet!")).toString() );
		return h;
	}
	@Override
	public boolean equals(Object o) {
		PiEntity e=(PiEntity)o;
		return side==e.side && intname.equals(e.intname);
	}

	// parse given stream to regular table
	protected static SimpleQueryHandler handleStream(InputStream is) throws IOException, SAXException {
		Parser p = new Parser();
		SimpleQueryHandler sqh = new SimpleQueryHandler();
		p.setContentHandler(sqh);
		p.parse(new InputSource(is));
		is.close();
		assert sqh.test() : "SimpleQueryHandler.test failed at PiEntity";
		return sqh;
	}

	public void parse_index(List<SWCV> rez, SimpleQueryHandler sqh) throws ParseException {
		assert rez!=null && sqh!=null : "PiEntity<SWCV> input check";
		int a=0, i;
		HashMap<String,String> kvm;
	    for (ArrayList<String> tr: sqh.rows) {
	    	i=0;
	    	kvm = new HashMap<String,String>(sqh.headers.size());
	    	for (String td: tr)
	    		kvm.put(sqh.headers.get(i++), td);
	    	rez.add(new SWCV(this, kvm));
	    	a++;
	    }
	    assert a==sqh.ia : "Not all given SWCV were parsed";
	}
	public void parse_index(List<PiObject> rez, SimpleQueryHandler sqh, boolean deleted) {
		int i=0, a=0;
	    int iRaw=-1,iOID=-1,iVID=-1;
	    for (String head: sqh.headers) {
	    	if ("Raw".equals(head)) 
	    		iRaw = i;
	    	else if ("OBJECTID".equals(head))
	    		iOID = i;
	    	else if ("VERSIONID".equals(head))
	    		iVID = i;
	    	i++;
		}
	    if (iRaw < 0 || iOID < 0 || iVID < 0)
	    	throw new RuntimeException("Either Raw/OID/VID not found");
	    for (ArrayList<String> tr: sqh.rows) {
	    	i=0;
	    	PiObject po = new PiObject(this,deleted);
			for (String td: tr) {
				if (i == iRaw)
					po.qryref = td;
				else if (i == iOID)
					po.objectid = UUtil.getBytesUUIDfromString(td);
				else if (i == iVID)
					po.versionid = UUtil.getBytesUUIDfromString(td);
				i++;
			}
			if (side==Side.Repository) {
				po.refSWCV = po.extrSwcvSp(host);
			}
			rez.add(po);
			a++;
		}
	    assert a==sqh.ia : "Not all given PiObject were parsed";
	}
	HTask makeOnlineHTask (PiHost p, boolean deleted) throws MalformedURLException, IOException {
		String nm = "idx_" + (deleted ? "deleted_" : "active_") + intname
				, qry = "", sync = "syncTabL=true&", dt="", deld="deletedL=D&", alive="deletedL=N&";
		if (lastDtFrom!=null) {
			Date d = new Date(lastDtFrom-10*86400*1000);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			dt = sdf.format(d);
			dt = "qcActiveL0=true&qcKeyL0=MODIFYDATE&qcOpL0=GE&qcValueL0=#DT#"+dt+"&";
		}
		if (intname.startsWith("ifm")) sync="";
		if (side.equals(Side.Repository) && !deleted)
			qry = "qc=All+software+components&" + sync + dt + alive +"xmlReleaseL=7.1&queryRequestXMLL=&types=" + intname + "&result=RA_XILINK&result=OBJECTID&result=VERSIONID&action=Start+query";
		else if (side.equals(Side.Repository))
			qry = "qc=All+software+components&" + sync + dt + deld +"xmlReleaseL=7.1&queryRequestXMLL=&types=" + intname + "&result=RA_XILINK&result=OBJECTID&result=VERSIONID&action=Start+query"; 
		else if (side.equals(Side.Directory) && !deleted)
			qry = "qc=Default+%28for+directory+objects%29&" + sync + dt + alive +"xmlReleaseL=7.1&types=" + intname + "&result=RA_XILINK&result=OBJECTID&result=VERSIONID&action=Start+query";
		else if (side.equals(Side.Directory))
			qry = "qc=Default+%28for+directory+objects%29&" + sync + dt + deld +"xmlReleaseL=7.1&types=" + intname + "&result=RA_XILINK&result=OBJECTID&result=VERSIONID&action=Start+query";
		else
			throw new RuntimeException("Unknown logic");
		
		if (minDT==0) minDT = new Date().getTime();
		HTask h = new HTask(nm, p.establishPOST(side.url(p.uroot), true), qry);
		return h;
	}
}

// ---------------------------------------------------------------------------------------------------
class PiObject {
	enum Kind {NULL, UNKNOWN, MODIFIED,};
	
	static String TPL_SWCV = "&VC=SWC&SWCGUID=", TPL_SP="&SP=";
	Kind kind = Kind.NULL;
	boolean is_dirty = true, deleted, inupdatequeue=false;
	PiEntity e;
	String   qryref;
	byte[]   objectid,versionid;
	long     refDB=-1, refSWCV=-1;
	FutureTask<HTask> task = null;
	PiObject previous = null;		// предыдущий найденный в БД

	Long refSWCVsql() {
		return refSWCV!=-1L ? refSWCV : null;
	}
	
	PiObject (){}
	PiObject (PiEntity e, boolean deleted) {
		this.e = e;
		this.deleted = deleted;
	}
	PiObject (PiEntity ent, ResultSet rs) throws SQLException {
		//sql_objver_getall
		//o.object_ref,o.swcv_ref,o.object_id,o.is_deleted,o.url_ext,v.version_id,v.is_active
		refDB = rs.getLong(1);
		refSWCV = rs.getObject(2)==null ? -1L : rs.getLong(2);
		objectid = rs.getBytes(3);
		deleted = rs.getLong(4) == 1;
		qryref = rs.getString(5);
		versionid = rs.getBytes(6);
		is_dirty = false;
		e = ent;
	}
	public String toString() {
		String s;
		s = qryref!=null && qryref.indexOf("/read/ext")>0 ? qryref.split("/read/ext?")[1] : qryref;
		s = e.title + "_" + refDB + ":" + refSWCV + (deleted?" DELETED":"") + " {" + UUtil.getStringUUIDfromBytes(objectid) + ", " + UUtil.getStringUUIDfromBytes(versionid) + "} " + s;
		return s;
	}
	public long extrSwcvSp(PiHost p) {
		if (e.side!=Side.Repository) return -1L;
		
		assert qryref!=null && !qryref.equals("");
		int i = qryref.indexOf(TPL_SWCV), j = qryref.indexOf(TPL_SP);
		assert i>0 && j>i : "URL parsing error for SWCV extraction: " + qryref; 
		String tswcv = qryref.substring(i+TPL_SWCV.length(),i+TPL_SWCV.length()+32);
		String tsp = qryref.substring(j+TPL_SP.length());
		i = tsp.indexOf('&');
		if (i>0) tsp = tsp.substring(0,i);
		
		byte swcvid[] = UUtil.getBytesUUIDfromString(tswcv);
		long sp = Long.parseLong(tsp);
		long ref = -1;
		for (SWCV s: p.swcv.values()) {
			if ((UUtil.areEquals(swcvid, s.ws_id)) && sp==s.sp) {
				return s.refDB;
			} 
		}
//		Object[] o = new Object[]{UUtil.getBytesUUIDfromString(swcv), Long.parseLong(sp)};
		return -1L;
	}
	public int equalAnother(PiObject an) {
		assert objectid!=null && an!=null && an.objectid!=null : "input check";
		boolean o= refSWCV==an.refSWCV && UUtil.areEquals(objectid, an.objectid), v = o && UUtil.areEquals(versionid, an.versionid);
		if (v)
			return 2; // objectid и versionid совпадают
		else if (o)
			return 1; // objectid совпадает, versionid нет
		else
			return 0; // объект не найден
	}
	public void pawtouch() {
		if (inupdatequeue) return;
		if (is_dirty) e.host.diffo.addPiObjectUpdateQueue(this);
	}
}

class SWCV extends PiObject {
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	// definition query template
	static String qdef="qc=All+software+components&syncTabL=true&deletedL=B&xmlReleaseL=7.1&queryRequestXMLL=&types=workspace&result=COMPONENT_NAME&result=COMPONENT_VENDOR&result=WS_ID&result=MODIFYDATE&result=MODIFYUSER&result=WS_NAME&result=CAPTION&result=ELEMENTTYPEID&result=NAME&result=VENDOR&result=VERSION&result=SWC_GUID&result=WS_TYPE&result=EDITABLE&result=ORIGINAL&result=ELEMENTTYPEID&result=DEVLINE&result=WS_ORDER&action=Start+query";
	// dependencies query template
	static String qdeps="qc=All+software+components&syncTabL=true&deletedL=B&xmlReleaseL=7.1&queryRequestXMLL=&types=workspace&result=DEPTYPE&result=WS_ID&result=DEPWS_ID&result=DEPWS_NAME&result=WS_ORDER&result=SEQNO&result=DEVLINE&action=Start+query";
	HashMap<String,String> kvm = null;

	boolean index_me = false; 
//	private static HashMap<Long,SWCV> refcache = new HashMap <Long,SWCV>(100); 

	String vendor, caption, name, modify_user, elementTypeId, versionset;
	byte ws_id[] = null;
	long sp;
	boolean is_editable, is_original;
	char type;
	Date modify_date = null;
	class DependentSWCV {
		long seqno,refDB=-1L,order=0;
		byte []dependent_id = null;
		String dependentName = null;
		boolean indb=false;
		DependentSWCV (HashMap<String,String> kvm) {
			assert kvm!=null : "DependentSWCV(kvm) input failed" ;
			refDB = -1;
			assert kvm.containsKey("Order") && kvm.containsKey("SeqNo") : "DependentSWCV has no good attributes";
			order = Long.parseLong(kvm.get("Order"));
			seqno = Long.parseLong(kvm.get("SeqNo"));
			dependent_id = UUtil.getBytesUUIDfromString(kvm.get("dependentWK Id"));
			dependentName = kvm.get("dependentWK Name");
		}
		DependentSWCV (ResultSet rs) throws SQLException {
			//depswcv_ref,seqno,order,depws_id,depws_name FROM swcvdep WHERE swcv_ref=?1;
			refDB = rs.getLong(1);
			seqno = rs.getLong(2);
			order = rs.getLong(3);
			dependent_id = rs.getBytes(4);
			dependentName = rs.getString(5);
		}
		public boolean equalDSWCV(DependentSWCV an, SWCV me, SWCV ot) {
			assert dependent_id!=null && an!=null && an.dependent_id!=null;
			return UUtil.areEquals(dependent_id, an.dependent_id) && sp==an.order 
					&& me.versionset.equals(ot.versionset);
		}
		public String toString() {
			return "ref=" + refDB + ",guid=" + UUtil.getStringUUIDfromBytes(dependent_id) + ",sp=" + sp +",seqno="+seqno;
		}
	}
	List<DependentSWCV> deps = null;
	public String toString() {
		String s = "SWCV{refdb="+refDB+",guid=" + UUtil.getStringUUIDfromBytes(ws_id)+",name="
			+name+" sp="+sp+" versionset="+versionset+","+
			caption+" "+type+" "+is_editable+"_"+is_original;
		if (deps!=null) for (DependentSWCV sd:deps) {
			s += "\n\tdependency: " + sd.toString();
		}
		return s;
	}
	public boolean equalAnother(SWCV an) {
		assert refDB!=-1L && an!=null;
		return UUtil.areEquals(ws_id, an.ws_id) && sp==an.sp;
	}
	public boolean equalattrSWCV(SWCV an) {
		assert false;
		return UUtil.areEquals(ws_id, an.ws_id) && sp==an.sp;
	}
	public boolean equalsDep(DependentSWCV dep) {
		assert refDB!=-1L && dep!=null;
		return UUtil.areEquals(ws_id, dep.dependent_id) && sp==dep.order;
	}
	private boolean is_sapb = false;
//	boolean is_unknown() {return this.ref==-1;}
	boolean is_sap() {return is_sapb;}

	SWCV(PiEntity e, HashMap<String,String> kvm) throws ParseException {
		super(e,false);
		this.kvm = kvm;
		vendor = kvm.get("Component Vendor");
		ws_id = UUtil.getBytesUUIDfromString(kvm.get("Id"));
		if (kvm.containsKey("ModifyDate")) modify_date = df.parse(kvm.get("ModifyDate"));
		sp = Long.parseLong(kvm.get("Order"));
		versionset = kvm.get("Versionset");
		elementTypeId = kvm.get("Swcv ElementTypeId");

		modify_user = kvm.get("ModifyUser");
		name = kvm.get("Name");
		caption = kvm.get("Swcv Caption");
		if (kvm.containsKey("Type")) type = kvm.get("Type").charAt(0);
		is_editable = Boolean.parseBoolean(kvm.get("isEditable"));
		is_original = Boolean.parseBoolean(kvm.get("isOriginal"));
		is_sapb = "sap.com".equals(vendor);
		is_dirty = true;
	}
	SWCV(PiEntity e, ResultSet rs) throws SQLException {
		super(e,false);
		this.kvm = null;
//		rs: s.swcv_ref,s.vendor,s.ws_id,s.sp,s.modify_date,s.modify_user,s.name,s.caption,\
//			s.type,s.is_editable,s.is_original,s.elementtypeid,s.versionset,s.index_me
		refDB = rs.getLong(1); 
		vendor = rs.getString(2);
		ws_id = rs.getBytes(3);
		sp = rs.getLong(4);
		modify_date = new Date(rs.getLong(5));
		modify_user = rs.getString(6);
		name = rs.getString(7);
		caption = rs.getString(8);
		type = rs.getString(9).charAt(0);
		is_editable = rs.getLong(10)==1; 
		is_original = rs.getLong(11)==1;
		elementTypeId = rs.getString(12);
		versionset = rs.getString(13);
		is_dirty = false;
//		refcache.put(refDB, this);
	}
//	void putCache() {
//		assert refDB!=-1L;
//		refcache.put(refDB, this);
//	}
//	static SWCV lookup(long ref) {
//		assert ref!=-1L;
//		return refcache.get(ref);
//	}
	void setInsertFields(PreparedStatement ins) throws SQLException {
//		ins: host_id,session_id,ws_id,type,vendor,caption,name,sp,
//			modify_date,modify_user,is_editable,is_original,elementtypeid,versionset,index_me
		DUtil.setStatementParams(ins, 
				e.host.host_id,
				e.host.diffo.session_id,
				ws_id,
				type,
				vendor,
				caption,
				name,
				sp,
				modify_date,
				modify_user,
				is_editable ? 1 : 0,
				is_original ? 1 : 0,
				elementTypeId,
				versionset,
				index_me ? 1 : 0
				);
	}
	void setInsertBatchDepFields(PreparedStatement insdep) throws SQLException {
		assert refDB>0 && insdep!=null: "input1 assert|" + refDB + insdep;
//		insdep: swcv_ref,order,seqno,depws_id,depws_name,session_id
		for (DependentSWCV dep: deps) {
			DUtil.setStatementParams(insdep, 
				refDB,
				dep.order,
				dep.seqno,
				dep.dependent_id,
				dep.dependentName,
				e.host.diffo.session_id
				);
			insdep.addBatch();
		}
	}
	
	void setDeps (List<SWCV> ext_dep) {
		assert ext_dep!=null && ext_dep.size()>0 : "input assert|"+ext_dep;
		assert deps==null : "non-initialized state";
		this.deps = new ArrayList<DependentSWCV>(10);
		List<SWCV> todel = new LinkedList<SWCV>();
		for (SWCV po: ext_dep) {
			DependentSWCV depSWCV = new DependentSWCV(po.kvm);
			byte[] ws_id2 = UUtil.getBytesUUIDfromString(kvm.get("Id"));
			if (UUtil.areEquals(ws_id, ws_id2) && sp==depSWCV.order && versionset.equals(po.versionset)) {
				deps.add(depSWCV);
				todel.add(po);
			}
		}
		for (SWCV t: todel) ext_dep.remove(t);
	}
	
	void setDeps (ResultSet rs) throws SQLException {
		assert deps==null : "non-initialized state";
		deps = new ArrayList<DependentSWCV>(10);
		while (rs.next()) {
			DependentSWCV depSWCV = new DependentSWCV(rs);
			deps.add(depSWCV);
		}
	}	
	// 
//	protected void alignDep(List<SWCV> dict) {
//		assert ref!=-1L : "SWCV isn't in database yet (" + UUtil.getStringUUIDfromBytes(ws_id) + ",sp=" + sp + ")";
//		// проставляет ссылки на swcv_ref
//		if (deps!=null)	for (DependentSWCV d: deps) {
//			d.indb = false;
//			for (SWCV s: dict)
//				if (s.sp==d.sp && UUtil.areEquals(s.ws_id, d.dependent_id)) d.ref = s.ref;
//		}
//	}
//	boolean markDepDb(long dep, long seqno, byte[] depid, String depname) {
//		boolean b = false;
//		if (deps!=null)	for (DependentSWCV d: deps) {
//			b = (d.ref != -1L && d.ref==dep && d.seqno==seqno) ||
//				(d.ref==-1L && UUtil.areEquals(d.dependent_id, depid));
//			if (b && !d.indb) { 
//				d.indb = b;
//				return b;
//			}
//		}
//		return b;
//	}
//	boolean areAllMarked() {
//		boolean b = true;
//		if (deps!=null)	for (DependentSWCV d: deps) b = b && d.indb;
//		return b;
//	}
//	void putDeps(PreparedStatement ins, long session_id) throws SQLException {
//		if (deps!=null)	for (DependentSWCV d: deps) {
//			DUtil.setStatementParams(ins, ref, d.ref!=-1L ? d.ref : null, d.seqno, d.dependent_id, d.dependentName, session_id);
//			ins.addBatch();
//		}
//	}
}
