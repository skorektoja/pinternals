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
//	static private boolean shutdown;

	public HUtil() {
		this(10);
	}
	public HUtil(int threads) {
		limit = threads;
		q = new ArrayBlockingQueue<Runnable>(limit);
//		shutdown = false;
		log.config("Registered HUtil, maxthreads=" + limit);
		ex = new ThreadPoolExecutor(limit, limit+10, 20, TimeUnit.SECONDS, q);
	}
	public static void shutdown() {
		ex.shutdown();
	}
	static public FutureTask<HTask> addHTask(HTask h)  {
//		assert incoming!=null : "Empty incoming queue. 'new HUtil()' was missed";
//		assert !incoming.contains(h) : "Attempt to insert already existed HTask with name=" + h.name;
//		Thread t = new Thread(h);
//		t.setName(h.name);
		h.ok = false;
		
		FutureTask<HTask> f = new FutureTask<HTask>(h);
		while (ex.getActiveCount() >= limit)
			try {
				ex.awaitTermination(1000, TimeUnit.MILLISECONDS);
			} catch (InterruptedException ignore) {}
		ex.execute(f);
//		incoming.add(t);
		if (log.isLoggable(Level.FINEST)) log.config("HUtil.addHTask for new task name=" + h.name + " URL:" + h.hc.getURL().toExternalForm());
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
		if (log.isLoggable(Level.FINEST)) 
			log.finest("HTask.connect(" + attempts + ") hash=" + this.hashCode());
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
		if (log.isLoggable(Level.FINE)) {
			log.fine("HTask.run(" + hc.getURL().toExternalForm() + ") " + method + " hash=" + this.hashCode());
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
		System.out.println("HTask run: " + name + " connect=" + ok);
		if (ok) {
			if (log.isLoggable(Level.FINE))
				log.fine("HTask.run( " + this.hashCode() + " ) ok");
			a = new ByteArrayOutputStream(300000); //hc.getContentLength() < 100 ? 300000 : hc.getContentLength());
			
			try {
				int i = hc.getInputStream().read();
				while (i!=-1) {
					a.write(i);
					if (fos!=null) fos.write(i);
					i = hc.getInputStream().read();
				}
				System.out.println("HTask run: " + name + " read OK bytes:" + a.size());
			} catch (IOException ex) {
				ok = false;
				ex.printStackTrace();
			}
		} else {
			  
		}
		if (fos!=null) fos.close();
		hc.disconnect();
		if (ok) {
			bis = new ByteArrayInputStream(a.toByteArray());
			if (log.isLoggable(Level.FINE))
				log.fine("HTask.run( " + hc.getURL().toExternalForm() + " " + method + " " + post + ") ok, bytes read: " + a.toByteArray().length);
		} else
			bis = null;
		return this;
	}
}

