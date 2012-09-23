package com.pinternals.diffo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
 
public class HUtil implements Runnable {
	static List<HTask> incoming;
	static HashMap<Integer,HTask> done;
	static int maxthreads, freethreads;
	static Thread handler;
	static private boolean shutdown;

	public HUtil() {
		this(10);
	}
	public HUtil(int threads) {
		maxthreads = threads;
		incoming = new LinkedList<HTask>();
		freethreads = maxthreads;
		handler = new Thread(this);
		done = new HashMap<Integer,HTask>(maxthreads*10);
		shutdown = false;
		handler.start();
	}
	public void run() {
		while (!shutdown) {
			if (!incoming.isEmpty() && freethreads > 0) {
				HTask h = incoming.get(0);
				incoming.remove(0);
				Thread t = new Thread(h);
				t.setName("" + t.getName() + " " + h.hc.getURL().toExternalForm());
				freethreads--;
				t.start();
			} else {
				Thread.yield();
			}
			try {Thread.sleep(100);} catch (InterruptedException ex) {}
		}
	}
	static boolean isDone(int f) {
		return done.containsKey(f);
	}
	static ByteArrayInputStream getBAIS(int f) {
		ByteArrayInputStream bais = done.get(f).getBAIS();
		done.put(f, null);
		return bais;
	}
	static synchronized void welldone(HTask h)  {
		assert done!=null : "Empty outgoing queue";
		done.put(h.hashCode(), h);
		freethreads++;
	}
	public static void shutdown() {
		shutdown = true;
	}
	static synchronized private int tick(HTask t) {
		assert incoming!=null : "Empty incoming queue";
		incoming.add(t);
		return t.hashCode();
	}
	static public int addGet(HttpURLConnection hc) {
		return tick(new HTask(hc));
	}
	static public int addPost(HttpURLConnection hc, String postData) {
		return tick(new HTask(hc, postData));
	}
	
}

class HTask implements Runnable {
	static int attempts403 = 15;
	HttpURLConnection hc;
	String method, post;
	int rc;
	ByteArrayOutputStream a = null;
	
	HTask (HttpURLConnection hc) {
		method = "GET";
		post = null;
		this.hc = hc;
	}
	HTask (HttpURLConnection hc, String post) {
		method = "POST";
		this.post = post;
		this.hc = hc;
	}
	protected ByteArrayInputStream getBAIS() {
		ByteArrayInputStream bais = new ByteArrayInputStream(a.toByteArray());
		return bais;
	}
	private boolean connect(int counts) {
		assert hc != null;
		int i = 0;
		while (i<counts) {
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
					i++;
				else
					return false;
			} catch (IOException e) {
				i++;
			}
		}
		return false;
	}

	public void run() {
		boolean b = connect(attempts403);
		if (b) {
			a = new ByteArrayOutputStream(1024);
			try {
				int i = hc.getInputStream().read();
				while (i!=-1) {
					a.write(i);
					i = hc.getInputStream().read();
				}
				hc.disconnect();
				HUtil.welldone(this);
			} catch (IOException ex) {
				hc.disconnect();
				HUtil.welldone(this);
				ex.printStackTrace();
			}
		} else {
			a = null;
			hc.disconnect();
			HUtil.welldone(this);
		}
	}
}
