package com.pinternals.diffo;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

import com.sap.lcr.api.cimclient.CIMClient;
import com.sap.lcr.api.cimclient.CIMOMClient;
import com.sap.lcr.api.cimclient.HttpRequestSender;
import com.sap.lcr.api.cimclient.LcrException;
import com.sap.lcr.api.cimname.CIMObjectReference;
import com.sap.lcr.api.sapmodel.SAP_BusinessSystem;
import com.sap.lcr.api.sapmodel.SAP_BusinessSystemAccessor;
import com.sap.lcr.api.sapmodel.SAP_BusinessSystemGuid;
import com.sap.lcr.api.sapmodel.SAP_BusinessSystemGuidAccessor;
import com.sap.lcr.api.sapmodel.SAP_BusinessSystemPath;
import com.sap.lcr.api.sapmodel.SAP_BusinessSystemPathAccessor;

public class SLD {
	protected static ArrayList<PiObject> getObjects(PiEntity e, URL u, String uname, String passwd)
	throws IOException {
		HttpRequestSender requestSender = new HttpRequestSender(u, uname, passwd);
		CIMOMClient cimomClient = new CIMOMClient(requestSender);
		CIMClient cc = new CIMClient(cimomClient);
		
		ArrayList<PiObject> a = new ArrayList<PiObject>(10); 
		try {
		if ("SAP_BusinessSystem".equals(e.intname)) 
			return SAP_BusinessSystem(e, cc);
		else if ("SAP_BusinessSystemGroup".equals(e.intname)) 
			return SAP_BusinessSystemGroup(e, cc);
		else if ("SAP_BusinessSystemPath".equals(e.intname)) 
			return SAP_BusinessSystemPath(e, cc);
		else if ("SAP_BusinessSystemGuid".equals(e.intname)) 
			return SAP_BusinessSystemGuid(e, cc);
		} catch (LcrException x) {
			throw new IOException(x);
		}
		return a;
	}
	
	private static ArrayList<PiObject> SAP_BusinessSystem(PiEntity e, CIMClient cc) 
	throws LcrException {
		ArrayList<PiObject> ao = new ArrayList<PiObject>(10);
		SAP_BusinessSystemAccessor bsa = new SAP_BusinessSystemAccessor(cc);
		SAP_BusinessSystem[] bs = bsa.enumerateSAP_BusinessSystemInstances();
		for (SAP_BusinessSystem x: bs) {
			PiObject o = new PiObject(e, false);
			o.kvm = new HashMap<String,String>(5);
			o.kvm.put("getCaption",x.getCaption());
//			o.kvm.put("getBusinessSystemName",x.getBusinessSystemName());
//			o.kvm.put("getBusinessSystemName_WT",x.getBusinessSystemName_WT());
//			o.kvm.put("getCaption_WT",x.getCaption_WT());
			o.kvm.put("Desc", x.getDescription());
			o.kvm.put("Name", x.getName());
//			o.kvm.put("cid", ""+ x.getCID() );
			o.kvm.put("lastchange","" + x.getTimeOfLastStateChange());
			
			
			
			System.out.println(o);
			ao.add(o);
		}
		return ao;
	}

	private static ArrayList<PiObject> SAP_BusinessSystemGroup(PiEntity e, CIMClient cc) 
	throws LcrException {
		ArrayList<PiObject> ao = new ArrayList<PiObject>(10);
		SAP_BusinessSystemAccessor bsa = new SAP_BusinessSystemAccessor(cc);
		SAP_BusinessSystem[] bs = bsa.enumerateSAP_BusinessSystemInstances();
		for (SAP_BusinessSystem x: bs) {
			PiObject o = new PiObject(e, false);
//			ao.add(o);
		}
		return ao;
	}
	private static ArrayList<PiObject> SAP_BusinessSystemPath(PiEntity e, CIMClient cc) 
	throws LcrException {
		ArrayList<PiObject> ao = new ArrayList<PiObject>(10);
		SAP_BusinessSystemAccessor bsa = new SAP_BusinessSystemAccessor(cc);
		SAP_BusinessSystem[] bs = bsa.enumerateSAP_BusinessSystemInstances();
		for (SAP_BusinessSystem x: bs) {
			PiObject o = new PiObject(e, false);
//			ao.add(o);
		}
		return ao;
	}
	private static ArrayList<PiObject> SAP_BusinessSystemGuid(PiEntity e, CIMClient cc) 
	throws LcrException {
		ArrayList<PiObject> ao = new ArrayList<PiObject>(10);
		SAP_BusinessSystemGuidAccessor bsa = new SAP_BusinessSystemGuidAccessor(cc);
		SAP_BusinessSystemGuid[] bs = bsa.enumerateSAP_BusinessSystemGuidInstances();
		for (SAP_BusinessSystemGuid x: bs) {
			PiObject o = new PiObject(e, false);
			o.kvm = new HashMap<String,String>(5);
//			o.kvm.put("getBusinessSystemName",x.getBusinessSystemName());
//			o.kvm.put("getBusinessSystemName_WT",x.getBusinessSystemName_WT());
//			o.kvm.put("getCaption_WT",x.getCaption_WT());
			System.out.println(o);
//			ao.add(o);
		}
		return ao;
	}

	
	
	protected static void getTransportGroup(URL u, String uname, String passwd)
	throws Exception {
		// здесь можно PING повесить
		HttpRequestSender requestSender = new HttpRequestSender(u, uname, passwd );
		CIMOMClient cimomClient = new CIMOMClient(requestSender);
		CIMClient cc = new CIMClient(cimomClient);

		SAP_BusinessSystemAccessor bsa = new SAP_BusinessSystemAccessor(cc);
		SAP_BusinessSystem[] bs = bsa.enumerateSAP_BusinessSystemInstances();
		for (SAP_BusinessSystem x: bs) {
			System.out.println(x.getCaption() + " " + x.getBusinessSystemName());
		}
		SAP_BusinessSystemPath[] ps = new SAP_BusinessSystemPathAccessor(cc).enumerateSAP_BusinessSystemPathInstances();
		System.out.println(ps + "\t" + ps.length);
		for (SAP_BusinessSystemPath x: ps) {
			CIMObjectReference from = x.getAntecedent(), to = x.getDependent();
			
		}
	}
}
