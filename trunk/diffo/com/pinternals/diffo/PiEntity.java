package com.pinternals.diffo;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import org.ccil.cowan.tagsoup.Parser;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

class CPACache {
	private static String pcc = "CPAObject: (Channel) keys: ObjectId="; 
	private HashSet<ByteBuffer> channelset;
	java.util.Date refreshed = null;
	
	public void parseHtml(String txt) {
		refreshed = new Date();
		channelset = new HashSet<ByteBuffer>(100);
		String lines[] = txt.split("\\n");
		byte b[];
		if (lines!=null) for (int i=0; i<lines.length; i++) {
			String s = lines[i].trim();
			if (s.startsWith(pcc)) {
				b = UUtil.getBytesUUIDfromString(s.substring(pcc.length(), pcc.length()+33));
	 			channelset.add(ByteBuffer.wrap(b));
			} 
		}
	}
	public boolean isChannelInCPA(byte b[]) {
		return channelset.contains(ByteBuffer.wrap(b));
	}
	public String toString() {
		return DUtil.format("CPA_toString", refreshed, channelset.size());
	}
	
}

class RawRef {
	// parts of query string, most useful
	// naive implementation
	public String key=null; // unescaped one!
	byte[] oid;
	RawRef (String queryString) throws UnsupportedEncodingException {
		assert queryString != null;
		String params[] = queryString.split("&");
		for (int i=0; i<params.length; i++) {
			String kv[] = params[i].split("=", 2);
			assert kv!=null && kv[0]!=null && kv[1]!=null;
			String v = new String(URLDecoder.decode(kv[1], "UTF-8"));
			if (kv[0].equals("KEY")) 
				key = v;
		}
	}
}

class PiObject {
	static String TPL_SWCV = "&VC=SWC&SWCGUID=", TPL_SP="&SP="; 
	boolean  deleted;
	PiEntity e;
	String   rawref;
	byte[]   objectid,versionid;
	long     refDB, refSWCV;
	HashMap<String,String> kvm = null;					// only for SWCV and SLD
	PiObject (PiEntity p, boolean deleted) {
		e = p;
		this.deleted = deleted;
	}
	public String toString() {
		String s;
		if (kvm==null) {
			s = rawref!=null && rawref.indexOf("/read/ext")>0 ? rawref.split("/read/ext?")[1] : rawref;
			s = e.title + "_" + refDB + ":" + refSWCV + (deleted?" DELETED":"") + " {" + UUtil.getStringUUIDfromBytes(objectid) + ", " + UUtil.getStringUUIDfromBytes(versionid) + "} " + s;
		} else {
			s = e.intname + " {";
			for (String k: kvm.keySet())
				s += k + ": " + kvm.get(k) + ", ";
			s += "}";
		}
		return s;
	}
	/** @return массив из {byte[] swcv, long sp} 
	 * 
	 */ 
	public Object[] extrSwcvSp() {
		assert rawref!=null && !rawref.equals("");
		int i = rawref.indexOf(TPL_SWCV), j = rawref.indexOf(TPL_SP);
		assert i>0 && j>i : "URL parsing error for SWCV extraction: " + rawref; 
		String swcv = rawref.substring(i+TPL_SWCV.length(),i+TPL_SWCV.length()+32);
		String sp = rawref.substring(j+TPL_SP.length());
		i = sp.indexOf('&');
		if (i>0) sp = sp.substring(0,i);
		Object[] o = new Object[]{UUtil.getBytesUUIDfromString(swcv), Long.parseLong(sp)};
		return o;
	}
}
class SWCV {
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	long ref = -1;
	boolean index_me = false; 

	String vendor, caption, name, modify_user;
	byte ws_id[], dependent_id[];
	Long sp, seqno;
	boolean is_editable, is_original;
	char type;
	Date modify_date;
	
	private boolean is_sapb = false;

	boolean is_unknown() {return this.ref==-1;}
	boolean is_sap() {return is_sapb;}

	SWCV (PiObject po) throws ParseException {
		vendor = po.kvm.get("Component Vendor");
		ws_id = UUtil.getBytesUUIDfromString(po.kvm.get("Id"));
		modify_date	= df.parse(po.kvm.get("ModifyDate"));

		modify_user = po.kvm.get("ModifyUser");
		name = po.kvm.get("Name");
		sp = Long.parseLong(po.kvm.get("Order"));
		seqno = Long.parseLong(po.kvm.get("SeqNo"));
		dependent_id = UUtil.getBytesUUIDfromString(po.kvm.get("dependentWK Id"));
		caption = po.kvm.get("Swcv Caption");
		type = po.kvm.get("Type").charAt(0);
		is_editable = Boolean.parseBoolean(po.kvm.get("isEditable"));
		is_original = Boolean.parseBoolean(po.kvm.get("isOriginal"));
		is_sapb = "sap.com".equals(vendor);
//		String s = "SWCV{vendor+" "+Util.getStringUUIDfromBytes(ws_id)+" "+
//			modify_date+" "+modify_user+" "+name+" "+sp+" "+seqno+" "+
//			Util.getStringUUIDfromBytes(dependent_id)+" "+caption+" "+type+" "+is_editable+"_"+is_original;
	}
}

class ResultAttribute {
	String internal, caption;
	int seqno;
	public ResultAttribute (String internal, String caption, int seqno) {
		this.internal = internal;
		this.caption = caption;
		this.seqno = seqno;
	}
}

public class PiEntity {
	public static String SWCV = "workspace", FOLDER="FOLDER";

	public String intname, title;
	public long entity_id;
	public int seqno;
	public Side side = null;
	public ArrayList<ResultAttribute> attrs = new ArrayList<ResultAttribute>(20);
	
	public PiEntity (long entity_id, Side side, String intname, String title, int seqno) {
		this.intname = intname;
		this.title = title;
		this.side = side;
		this.seqno = seqno;
		this.entity_id = entity_id;
	}
	@Override
	public boolean equals(Object o) {
		PiEntity e=(PiEntity)o;
		return side==e.side && intname.equals(e.intname);
	}

	public String getQueryPrepareParse() {
		String s = "";
		if (side==Side.Repository && intname.equals(SWCV)) {
			// нужен полный фарш для SWCV, поскольку у них нет ссылки [R]
			for (ResultAttribute ra: attrs) {
				s = s + "&result=" + ra.internal;
			}
		} else { // достаточно всего трёх таблеток! главное, не ударяться в истерику.
			s =  "result=RA_XILINK&result=OBJECTID&result=VERSIONID";
		}
		return s;
	}
	/* simple query handler */
	static class SQH extends DefaultHandler {
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
	/* simple query: entities and attributes */
	static class SQEntityAttr extends DefaultHandler {
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
	public static SQEntityAttr parse_ra(InputStream is, String n) throws IOException, SAXException {
		Parser p = new Parser();
		SQEntityAttr sq = new SQEntityAttr(n);
		p.setContentHandler(sq);
		p.parse(new InputSource(is));
		is.close();
		return sq;
	}

	public ArrayList<PiObject> parse_index(InputStream is, boolean deleted) 
	throws IOException, SAXException {
		// Часто могут идти огромные размеры для парсинга. Лучше переложиться в файл, парсить его 
		// и заранее знать размеры бедствия
	    ArrayList<PiObject> rez = null;
		Parser p = new Parser();
		SQH sqh = new SQH();
		p.setContentHandler(sqh);
		p.parse(new InputSource(is));
		is.close();
		assert sqh.test();
		int i=0, a=0;
	    if (side==Side.Repository && intname.equals(SWCV)) {
	    	rez = new ArrayList<PiObject>(sqh.ia+1);
		    for (ArrayList<String> tr: sqh.rows) {
		    	PiObject po = new PiObject(this,deleted);
		    	i=0;
		    	po.kvm = new HashMap<String,String>(sqh.headers.size());
		    	for (String td: tr) {
		    		po.kvm.put(sqh.headers.get(i++), td);
		    	}
		    	rez.add(po);
		    	a++;
		    }
		    assert a==sqh.ia;
		    sqh = null;
	    } else {
	    	// not SWCV
	    	rez = new ArrayList<PiObject>(sqh.ia+1);
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
						po.rawref = td;
					else if (i == iOID)
						po.objectid = UUtil.getBytesUUIDfromString(td);
					else if (i == iVID)
						po.versionid = UUtil.getBytesUUIDfromString(td);
					i++;
				}
				rez.add(po);
				a++;
			}
		    assert a==sqh.ia;
	    }
	    return rez;
	}
}
