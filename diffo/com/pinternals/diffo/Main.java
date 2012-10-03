package com.pinternals.diffo;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.LogManager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.xml.sax.SAXException;

import com.pinternals.diffo.impl.DifferencerNode;

public class Main {
	@SuppressWarnings("unused")
	private static void checkInputStream(InputStream gis) throws IOException {
		if (gis==null) return;
		int i = gis.read(), z=0;
		while (i!=-1) {
			i = gis.read();
			z++;
		}
		assert z!=0;
	}
	
	public static void eclipser (String dbname) {
		try {
			LogManager.getLogManager().readConfiguration(Main.class.getResourceAsStream("/logging.properties"));
			Diffo d = new Diffo(dbname, null, 10);
			
			if (d.opendb() && (d.isDbExist() || d.createdb()) && d.validatedb()) {
				if (!d.start_session()) 
					System.err.println("session not started!");
				else {
					System.out.println("Session " + d.session_id + " started OK");
					PiHost xid = d.addPiHost("XID", "http://tralala");
					PiHost xiq = d.addPiHost("XIQ", "http://blahbla");

					List<DiffItem> l, r;
					d.refreshMeta(xid);
					d.refreshMeta(xiq);

					Side[] sides={Side.Repository, Side.Directory, Side.SLD};
					for (Side s: sides) {
						System.out.println("\n" + s + "\n====================");
						for (PiEntity e: xid.entities.values()) 
							if (e.side==s) {
								l = d.list(xid, e);
								if (l.size()>0) { 
//									for (DiffItem di: l) checkInputStream(di.getPayload());
									r = d.list(xiq, xiq.getEntity(e.side, e.intname));
//									for (DiffItem di: r) checkInputStream(di.getPayload());
									System.out.println("[" + e.intname + "] " + e.title + "\t: " + l.size() + "\t" + r.size());
								} else
									r = null;
						}
					}
					// 
					d.finish_session();
				}
				d.closedb();
			} else {
				System.err.println("ERROR: DB " + dbname + " neither opened nor created");
			} 
		} catch (Exception e) {
			System.err.println("Exception:" + e.getMessage());
			e.printStackTrace();
		}
		
	}

	public static void dbtester (String dbname) {
		try {
			LogManager.getLogManager().readConfiguration(Main.class.getResourceAsStream("/logging.properties"));
			Diffo d = new Diffo(dbname, null, 10);
			
			if (d.opendb() && (d.isDbExist() || d.createdb()) && d.validatedb()) {
				if (!d.start_session()) 
					System.err.println("session not started!");
				else {
					System.out.println("Session " + d.session_id + " started OK");
					@SuppressWarnings("unused")
					PiHost xid = d.addPiHost("XID", "http://somewhere:50000");
					@SuppressWarnings("unused")
					PiHost xiq = d.addPiHost("XIQ", "http://nowhere:50000");

					boolean b = d.createdb();
					System.out.println("Database re-created: " + b);
					// 
					d.finish_session();
				}
				d.closedb();
			} else {
				System.err.println("ERROR: DB " + dbname + " neither opened nor created");
			} 
		} catch (Exception e) {
			System.err.println("Exception:" + e.getMessage());
			e.printStackTrace();
		}
	}

	public static void sample (String dbname) {
		try {
			LogManager.getLogManager().readConfiguration(Main.class.getResourceAsStream("/logging.properties"));
			Proxy prx = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("127.0.0.1", 8888));
			Diffo d = new Diffo(dbname, prx, 10);
			Diffo.simulatedb();
			
			if (d.opendb() && (d.isDbExist() || d.createdb()) && d.validatedb()) {
				if (!d.start_session()) 
					System.err.println("session not started!");
				else {
					System.out.println("Session " + d.session_id + " started OK");

					PiHost xid = d.addPiHost("DEV", "http://host_dev:50000");
					xid.setUserCredentials("pireport", "password");
//					d.fullFarsch(xid);

					PiHost xiq = d.addPiHost("QAS", "http://host_qas:50000");
					xiq.setUserCredentials("pireport", "password");
//					d.fullFarsch(xiq);
					
					d.finish_session();
				}
				d.closedb();
			} else {
				System.err.println("ERROR: DB " + dbname + " neither opened nor created");
			} 
		} catch (Exception e) {
			System.err.println("Exception:" + e.getMessage());
			e.printStackTrace();
		}
	}
	
	private static void transportCheck(Diffo d, PiHost p) 
	throws SQLException, IOException, SAXException, ParseException, InterruptedException, ExecutionException {
		d.refreshMeta(p);
		List<DiffItem> al;
		al = d.list(p, Side.Repository, "XI_TRAFO");
		al.addAll(d.list(p, Side.Repository, "MAPPING"));
		al = d.list(p, Side.Directory, "MappingRelation");
		for (DiffItem x: al) {
			DifferencerNode n = new DifferencerNode(x);
			boolean b = n.refresh(d, false);
			assert b;
			String s = "\nid:\t" + n.getObjectID() 
					+ "\nname:\t" + n.getObjectName()
					+ "\nnamesp:\t" + n.getObjectNamespace()
					+ "\ntype:\t" + n.getObjectType()
					+ "\nsuf:\t" + n.getTransportSuffix()
					+ "\nswcv:\t" + n.getObjectSWCV()
					+ "";
			System.out.println(s);
		}
	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			LogManager.getLogManager().readConfiguration(Main.class.getResourceAsStream("/logging.properties"));
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		
		int tx=5, th=30;
		Options opts = new Options();
		opts.addOption("h", "help", false, "help");
		opts.addOption("v", "version", false, "print the version information and exit");

		opts.addOption("d", "dbfile", true, "database file (diffo.db by default)");
		opts.addOption("s", "sid", true, "system ID" );
		opts.addOption("x", "xihost", true, "URL as http://first:50000 or https://second:59900" );
		opts.addOption("u", "uname", true, "user name");
		opts.addOption("p", "passwd", true, "password");
		opts.addOption(null, "tx", true, "thread indexing parallel (default is " + tx + ")");
		opts.addOption(null, "th", true, "thread http parallel (default is " + th + ")");
		
		OptionBuilder.withLongOpt("http-proxy-host");
		opts.addOption(null, "http-proxy-host", true, "http proxy hostname or IP address");
		opts.addOption(null, "http-proxy-port", true, "http proxy port");
		opts.addOption(null, "http-proxy-user", true, "user");
		opts.addOption(null, "http-proxy-pass", true, "passwd");

		CommandLine cmd = null;
		try {
			cmd = new PosixParser().parse(opts, args);
		} catch (org.apache.commons.cli.ParseException exp) {
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
			return;
		}
		// assertion test. Turn them on (java -esa -ea ...)
		boolean aoff = true;
		try {
			assert false;
		} catch (AssertionError e) {
			aoff = false;
		}
		if (aoff) {
			System.err.println("Asserts are switched off. Turn them on, please!");
			return;
		}
		assert UUtil.assertion();
		assert DUtil.assertion();
		
		String sid=null, xihost=null, uname=null, passwd=null, dbfn="diffo.db";
		Proxy prx = null;

		if (cmd.hasOption("help") || 
				( ( cmd.getOptions()==null || cmd.getOptions().length==0 ) &&
				  ( cmd.getArgs()==null || cmd.getArgs().length==0) ) ) { 
			new HelpFormatter().printHelp( "diffo", opts, true );
			return;
		}
		if (cmd.hasOption("version"))
			System.out.println("diffo version " + Diffo.version);
		if (cmd.hasOption("dbfile")) dbfn = cmd.getOptionValue("dbfile");
		if (cmd.hasOption("sid")) sid = cmd.getOptionValue("sid");
		if (cmd.hasOption("xihost")) xihost = cmd.getOptionValue("xihost");
		if (cmd.hasOption("uname")) uname = cmd.getOptionValue("uname");
		if (cmd.hasOption("passwd")) passwd = cmd.getOptionValue("passwd");
		if (cmd.hasOption("tx")) tx = Integer.parseInt(cmd.getOptionValue("tx"));
		if (cmd.hasOption("th")) th = Integer.parseInt(cmd.getOptionValue("th"));
		
//		boolean e = sid==null && xihost==null && uname==null && passwd==null;
		if (cmd.getArgs()==null || cmd.getArgs().length==0) {
			System.out.println("No command was specified. Exit.");
			return;
		}
		
		new HUtil(th);
		Diffo d = new Diffo(dbfn, prx, tx);
		PiHost pih = null;
		HierRoot root = null;
		HierSide hrep = null, hdir = null;
		boolean started = false, b;
		try {
			assert Diffo.simulatedb() : "SimulateDB error";
			for (String a0: cmd.getArgs()) {
				if ("start".equals(a0)) {
					if (started) continue;
					b = d.opendb() && (d.isDbExist() || d.createdb() && d.validatedb());
					started = b && d.start_session();
					if (started)
						System.out.println("Session " + d.session_id + " started OK");
					else
						System.err.println("Can't start session. Database was opened/created: " + b);
				} else if ("finish".equals(a0)) {
					if (!started) continue;
					d.finish_session();
					d.validatedb();
					d.closedb();
					started = false;
				} else if ("addHost".equals(a0)) {
					pih = d.addPiHost(sid, xihost);
					pih.setUserCredentials(uname, passwd);
				} else if ("refresh".equals(a0)) {
					assert pih!=null : "addHost wasn't called before refresh";
					root = root==null ? new HierRoot(d,pih) : root;
					d.refreshMeta(pih);
					d.__refreshSWCV(pih, false);

					System.out.println("+++++ Repository");
					hrep = root.addSide(Side.Repository);
					System.out.println("+++++ Directory");
					hdir = root.addSide(Side.Directory);

					for (HierSide s: root.sides) 
						for (PiEntity v: pih.entities.values())
							if (v.side == s.side) {
								HierEnt he = s.addPiEntity(v);
								he.getObjectsIndex();
								if (he.objs!=null) for (PiObject o: he.objs) {
									o.pawtouch();
								}
							}
					d.loopUpdateQueue();
					
				} else if ("prepare".equals(a0)) {
					assert pih!=null : "addHost wasn't called before refresh";
					root = root==null ? new HierRoot(d,pih) : root;
					d.refreshMeta(pih);
					d.__refreshSWCV(pih, false);
				} else if ("refresh(Repository,ariscxnocc)".equals(a0)) {
					assert pih!=null : "addHost wasn't called before refresh";
					assert root!=null : "prepare wasn't called before";
					hrep = hrep==null ? root.addSide(Side.Repository) : hrep;
					
					HierEnt he = hrep.addPiEntity(pih.getEntity(hrep.side, "ariscxnocc"));
					he.getObjectsIndex();
					if (he.objs!=null) for (PiObject o: he.objs) {
						o.pawtouch();
					}
					d.loopUpdateQueue();
				} else if (!a0.isEmpty() && a0.matches("refresh\\((Repository|Directory),[a-zA-Z_]+\\)")) {
					String s1, s2[];
					s1 = a0.substring("refresh(".length());
					s1 = s1.substring(0, s1.length()-1);
					s2 = s1.split(",");
					Side sd = Side.get(s2[0]);
					s1 = s2[1];
					PiEntity e = pih.getEntity(sd, s1);
					assert e!=null : "Entity " + s2[0] + "/" + s2[1] + " isn't detected";
				} else if ("transportCheck".equals(a0)) {
					transportCheck(d, pih);
				} else if ("migrateHostDB".equals(a0)) {
					if (pih==null) {
						pih = d.addPiHost(sid, xihost);
					}
					migrateHostDB(d, pih, "newHostDB");
				} else if ("migrateMainDB".equals(a0)) {
					d.migrateMainDB("newMainDB.tmp");
				} else {
					System.err.println("\n\n!!!! Unknown command: " + a0 + "\n\n");
					break;
				}
			}
			if (started) {
				started = false;
				d.finish_session();
				d.validatedb();
				d.closedb();
			}
		} catch (Exception ex) {
			d.shutdown();
			System.err.println("Exception: " + ex.getMessage());
			ex.printStackTrace();
		}
	}

	static void migrateHostDB(Diffo d, PiHost p, String newdb) throws SQLException {
		assert d!=null;
		p.migrateHostDB(newdb);
	}

}
