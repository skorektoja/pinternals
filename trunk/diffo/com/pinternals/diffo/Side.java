package com.pinternals.diffo;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Formatter;

public enum Side  {
	Repository
	,Directory
	,RWB
	,CPACache
	,ExchangeProfile
	,ARFCDES
	,JRFCDES
	,SLD
	;
	protected static String C_REPSUF = "/rep/support/SimpleQuery"
		, C_DIRSUF = "/dir/support/SimpleQuery"
		// those urls are stolen from ...j2ee\cluster\apps\sap.com\com.sap.aii.af.app\servlet_jsp\AdapterFramework\com.sap.aii.af.war\scheduler and so on 
		, C_AF_S = "/AdapterFramework/ChannelAdminServlet?action=%s&channelID=%s"
		, C_CPA_S = "/CPACache/monitor.jsp"
		, C_AF_SCH_XML = "/AdapterFramework/scheduler/scheduler.jsp?xml&tz"
//		, C_AF_SCH_HTML = "/AdapterFramework/scheduler/scheduler.jsp?tz"
		;
	private String defurl() {
		switch (this) {
		case Directory: 	return C_DIRSUF; //"/dir/start/index.jsp";
		case Repository:	return C_REPSUF; //"/rep/start/index.jsp";
		case SLD:			
			//return "/sld/cimom" // not good for pi.esworkplace
			return "/webdynpro/dispatcher/sap.com/tc~sld~wd~main/Main";
		default:
			throw new RuntimeException("for side: " + this + " there is no defurl");
		}
	}
	protected static Side get(String s) {
		//TODO: сделать через статический HashMap или Hashtable
		if (s.equals(Repository.txt())) return Repository;
		if (s.equals(Directory.txt())) return Directory;
		assert false : "Not implemented yet";
		return null;
	}
	protected String txt() {
		return this.toString();
	}
	protected URL url(URL root) throws MalformedURLException {
		URL u = new URL(root.toExternalForm() + defurl());
		return u;
	}
	protected static URL askCcStatus(URL u, String action, byte[] object_id) throws MalformedURLException {
		String uq = new Formatter().format(C_AF_S, action, UUtil.getStringUUIDfromBytes(object_id)).toString();
		return new URL(u.toExternalForm() + uq);
	}
	
	protected String createQueryResultAttributes(String intname) {
		switch (this) {
		case Directory:
			return "qc=Default+(for+directory+objects)&syncTabL=true&deletedL=B&xmlReleaseL=7.1&types=" + intname + "&action=Refresh+depended+values";
		case Repository:
			return "qc=All+software+components&syncTabL=true&deletedL=B&xmlReleaseL=7.1&queryRequestXMLL=&types=" + intname + "&action=Refresh+depended+values";
		default:
			throw new RuntimeException("for side: " + this + " there are no SimpleQuery result attributes");
		}
	}
}
