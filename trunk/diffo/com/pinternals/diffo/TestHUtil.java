package com.pinternals.diffo;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.FutureTask;
import java.util.logging.Logger;

import sun.misc.BASE64Encoder;

public class TestHUtil {
	private static Logger log = Logger.getLogger(TestHUtil.class.getName());
	public static void main(String[] args) {
		try {
			String basicAuth = "Basic " + new String(new BASE64Encoder().encode("IKUZNETSOV:12345678".getBytes()));
			
			new DUtil(20);
			for (int i=0; i<1; i++) {
				URL u = new URL("http://nettuno:50000/rep/support/SimpleQuery");
				HttpURLConnection hu = DUtil.getHttpConnection(null, u, 10000);
				hu.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
				hu.setRequestMethod("POST");
				hu.setRequestProperty("Authorization", basicAuth);
				
				String p = "qc=All+software+components&syncTabL=true&deletedL=B&xmlReleaseL=7.1&queryRequestXMLL=&types=type&action=Refresh+depended+values";
				HTask h = new HTask("test1", hu, p);
				System.out.println("01)HTask=" + h);
				
				FutureTask<HTask> t = DUtil.addHTask(h);
				System.out.println("02)Future=" + t);
				h = t.get();
				System.out.println("03)HTask=" + h);
				if (!h.ok) {
					h.reset(DUtil.getHttpConnection(null, u, 10000));
					h.hc.setRequestProperty("Authorization", basicAuth);
					t = DUtil.addHTask(h.reset(DUtil.getHttpConnection(null, u, 10000)));
					h = t.get();
					System.out.println("05)HTask=" + h);
				}
//					Object o = t.get();
//					System.out.println("Future.get()=" + o);
			}
			DUtil.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
