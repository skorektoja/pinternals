package com.pinternals.diffo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

public class DiffItem {
	private static Logger log = Logger.getLogger(DiffItem.class.getName());
	public String name;
	public byte[] version_id, object_id;
	public boolean deleted, db = false;
	public long oref;
	public PiHost p;
	protected Diffo d;
	
	private HashMap<String,String> hmTN = null;
	
	public DiffItem (Diffo d, PiHost p, String n, byte[] oid, byte[] vid, long ref, boolean del) {
		name = n;
		object_id = oid;
		version_id = vid;
		this.p = p;
		this.d = d;
		this.deleted = del;
		oref = ref;
	}
	
	public InputStream getPayload() throws SQLException, IOException {
		if (log.isLoggable(Level.CONFIG))
			log.config("Trying to get payload for object " + oref + " version " + UUtil.getStringUUIDfromBytes(version_id) + ", host " + p.host_id);
		ResultSet rs = p.getPayload(oref, version_id);
		byte[] gzip = null, oid = null;
		while (rs.next()) {
			gzip = rs.getBytes(1);
			oid = rs.getBytes(2);
			boolean dirty = rs.getLong(3)==1;
			assert !dirty : "Attempt to get dirty payload for object " + oref + " " + UUtil.getStringUUIDfromBytes(version_id);
			assert UUtil.areEquals(oid, object_id) : "Objects are not equals";
		}
		assert deleted || gzip!=null : "Not any single record fetched for alive object";
		if (deleted) return null;
		ByteArrayInputStream bis = new ByteArrayInputStream(gzip); 
		GZIPInputStream gis = new GZIPInputStream(bis);
		return gis;
	}
	
	public String getTransportAttr(String an)  {
		if (hmTN==null) 
			try {
				hmTN = d.readTransportNames(oref);
			} catch (Exception e) {
				throw new RuntimeException("Error when read transport attributes for object " + oref);
			}
		return hmTN.get(an);
	}
//	public String getSWCV() {
//		assert oref!=0 : "Object reference (oref) must be set before getting SWCV";
//		assert db : "DiffItem is not retrieved from DB yet";
//		return UUtil.getStringUUIDfromBytes(swcvid);
//	}
	
}
