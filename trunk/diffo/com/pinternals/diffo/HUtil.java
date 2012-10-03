package com.pinternals.diffo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
 
public class HUtil {
	private static Logger log = Logger.getLogger(HUtil.class.getName());
	static int limit;
	static BlockingQueue<Runnable> q = null;
	static ThreadPoolExecutor ex = null;

	public HUtil() {
		this(10);
	}
	public HUtil(int threads) {
		limit = threads;
		q = new ArrayBlockingQueue<Runnable>(limit+2000);
		log.config(DUtil.format("HUtil01",limit));
		ex = new ThreadPoolExecutor(limit, limit+5, 5, TimeUnit.SECONDS, q);
	}
	public static void shutdown() {
		log.config(DUtil.format("HUtil02"));
		ex.shutdown();
	}
	static public FutureTask<HTask> addHTask(HTask h)  {
		if (log.isLoggable(Level.FINE))
			log.fine(DUtil.format("HUtil03addHTask", h.name));
		h.ok = false;
		FutureTask<HTask> f = new FutureTask<HTask>(h);
		int i=0;
		while (ex.getPoolSize() < 10 && i++<10)
			try {
				ex.awaitTermination(10, TimeUnit.MILLISECONDS);
			} catch (InterruptedException ignore) {
				break;
			}
		ex.execute(f);
		return f;
	}
}
class HTask implements Callable<HTask> {
	private static Logger log = Logger.getLogger(HTask.class.getName());
	Object refObj = null;
	HttpURLConnection hc;
	String name, method, post;
	int rc, attempts=0;
	ByteArrayInputStream bis = null;
	boolean ok = false;
	
	HTask (String name, HttpURLConnection hc) {
		this.name = name;
		method = "GET";
		post = null;
		assert hc.getRequestMethod().equals(method) : "method mismatch";
		this.hc = hc;
	}
	HTask (String name, HttpURLConnection hc, String post) {
		this.name = name;
		method = "POST";
		assert hc.getRequestMethod().equals(method) : "method mismatch";
		this.post = post;
		this.hc = hc;
	}
	void connect() {
		assert hc != null : "Null HttpURLConnection for HTask";
		ok = false;
		if (log.isLoggable(Level.FINE))
			log.fine(DUtil.format("HTask04connect", name, hc.getURL().toExternalForm()));
		rc = -1;
		try {
			if ("POST".equals(method))
				DUtil.putPOST(hc, post);
			hc.connect();
			rc = hc.getResponseCode();
			ok = (rc == HttpURLConnection.HTTP_OK);
		} catch (IOException e) {
			ok = false;
			e.printStackTrace();
		}
	}
	public HTask call() {
		ByteArrayOutputStream a = null;
		File flg = new File("htask_" + name + "." + hashCode() + ".html");
		PrintStream fos = null;
		if (log.isLoggable(Level.FINEST)) {
			try { 
				flg.createNewFile();
				fos = new PrintStream(flg);
				fos.println("Http task " + name + "/" + method);
				if ("POST".equals(method)) 
					fos.println(post);
			} catch (Exception ignore) {
			}
		}
		connect();
		if (ok) {
			if (log.isLoggable(Level.CONFIG))
				log.config(DUtil.format("HTask05call", name));
			a = new ByteArrayOutputStream(1024);
			try {
				int i = hc.getInputStream().read();
				while (i!=-1) {
					a.write(i);
					if (fos!=null) fos.write(i);
					i = hc.getInputStream().read();
				}
			} catch (IOException ex) {
				ok = false;
				log.throwing(HTask.class.getCanonicalName(), "call", ex);
			}
		} else {
			log.severe(DUtil.format("HTask06call", name, rc));
			a = new ByteArrayOutputStream(1024);
			try {
				int i = hc.getErrorStream().read();
				while (i!=-1) {
					a.write(i);
					if (fos!=null) fos.write(i);
					i = hc.getErrorStream().read();
				}
			} catch (IOException ex) {
				log.throwing(HTask.class.getCanonicalName(), "call", ex);
			}
		}
		if (fos!=null) fos.close();
		hc.disconnect();
		
		bis = new ByteArrayInputStream(a.toByteArray());
		return this;
	}
}

