package com.pinternals.diffo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
 
public class HUtil implements Runnable {
	private static Logger log = Logger.getLogger(HUtil.class.getName());
	static List<Thread> incoming;
	static int maxthreads, freethreads;
	static Thread dispatcher;
	static private boolean shutdown;

	public HUtil() {
		this(10);
	}
	public HUtil(int threads) {
		maxthreads = threads;
		incoming = new LinkedList<Thread>();
		freethreads = maxthreads;
		dispatcher = new Thread(this);
		shutdown = false;
		log.config("Registered HUtil, maxthreads=" + maxthreads);
		dispatcher.start();
	}
	public static void join(Thread w) throws InterruptedException {
		while (incoming.contains(w))
			Thread.yield();
		w.join();
	}
	public void run() {
		while (!shutdown) {
			if (!incoming.isEmpty() && freethreads > 0) {
				Thread t = incoming.remove(0);
//				if (log.isLoggable(Level.FINEST)) log.config("HUtil.run() for new task " + h.hc.getURL().toExternalForm());
				freethreads--;
				t.start();
			}
			try {Thread.sleep(10L);} catch (InterruptedException ex) {}
		}
	}
	static synchronized void callback(HTask ht)  {
		if (log.isLoggable(Level.FINE))
			log.fine("HUtil.callback ok=" + ht.ok + " for name=" + ht.name + " hash=" + ht.hashCode() + " rc=" + ht.rc);
		incoming.remove(ht);
		freethreads++;
		if (ht.listener!=null) ht.listener.finished(ht);
	}
	public static void shutdown() {
		shutdown = true;
	}
	static synchronized public Thread addHTask(HTask h) {
		assert incoming!=null : "Empty incoming queue. 'new HUtil()' was missed";
		assert !incoming.contains(h) : "Attempt to insert already existed HTask with name=" + h.name;
		Thread t = new Thread(h);
		t.setName(h.name);
		incoming.add(t);
		if (log.isLoggable(Level.FINEST)) log.config("HUtil.addHTask for new task name=" + h.name + " URL:" + h.hc.getURL().toExternalForm());
		return t;
	}
}
interface HTaskListener {
	public void finished(HTask h);
}
class HTask implements Runnable {
	private static Logger log = Logger.getLogger(HTask.class.getName());
	static int attempts403 = 15;
	HttpURLConnection hc;
	String name, method, post;
	int rc, attempts=0;
	ByteArrayInputStream bis = null;
	boolean ok = false;
	HTaskListener listener = null;
	
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
	void setListener(HTaskListener l) {
		listener = l;
	}
	private boolean connect(int maxcounts) {
		assert hc != null;
		attempts = 0;
		while (attempts<maxcounts) {
			if (log.isLoggable(Level.FINEST)) 
				log.finest("HTask.connect(" + attempts + ") hash=" + this.hashCode());
			rc = -1;
			try {
				if ("POST".equals(method)) {
					DUtil.putPOST(hc, post);
				}
				hc.connect();
				rc = hc.getResponseCode();
				if (rc == HttpURLConnection.HTTP_OK) 
					return true;
				else if (rc == HttpURLConnection.HTTP_FORBIDDEN)
					attempts++;
				else
					return false;
			} catch (IOException e) {
				attempts++;
			}
		}
		return false;
	}
	public void run() {
		ByteArrayOutputStream a = null;
		if (log.isLoggable(Level.FINE))
			log.fine("HTask.run(" + hc.getURL().toExternalForm() + ") " + method + " hash=" + this.hashCode());
		ok = connect(attempts403);
		if (ok) {
			if (log.isLoggable(Level.FINE))
				log.fine("HTask.run( " + this.hashCode() + " ) ok");
			a = new ByteArrayOutputStream(hc.getContentLength() < 100 ? 100000 : hc.getContentLength());
			try {
				int i = hc.getInputStream().read();
				while (i!=-1) {
					a.write(i);
					i = hc.getInputStream().read();
				}
			} catch (IOException ex) {
				ok = false;
				ex.printStackTrace();
			}
		}
		hc.disconnect();
		if (ok) {
			bis = new ByteArrayInputStream(a.toByteArray());
			if (log.isLoggable(Level.FINE))
				log.fine("HTask.run( " + this.hashCode() + " ) ok, bytes read: " + a.toByteArray().length);
		} else
			bis = null;
		HUtil.callback(this);
	}
}
