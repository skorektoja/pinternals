package com.pinternals.diffo;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.logging.Logger;

import sun.misc.BASE64Encoder;

public class TestHUtil {
	private static Logger log = Logger.getLogger(TestHUtil.class.getName());
	public static void main(String[] args) {
		try {
			String basicAuth = "Basic " + new String(new BASE64Encoder().encode("IKUZNETSOV:12345678".getBytes()));
			
			new HUtil(2);
			for (int i=0; i<10; i++) {
				URL u = new URL("http://nettuno:50000/rep/support/SimpleQuery");
				HttpURLConnection hu = DUtil.getHttpConnection(null, u, 10000);
				hu.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
				hu.setRequestMethod("POST");
				
				String p = "qc=All+software+components&syncTabL=true&deletedL=B&xmlReleaseL=7.1&queryRequestXMLL=&types=type&action=Refresh+depended+values";
				HTask h = new HTask("test1", hu, p);
				System.out.println("HTask=" + h);
				
				if (!true) {
					h.call();
					System.out.println(h.bis);
				} else {
					FutureTask<HTask> t = HUtil.addHTask(h);
					System.out.println("Future=" + t);
//					Object o = t.get();
//					System.out.println("Future.get()=" + o);
				}
			}
//			Future<?> t = HUtil.addHTask(h);
//			System.out.println(h);
//			Object o = t.get();
//			System.out.println(o);
//			HUtil.addGet(h);
//			HUtil.addGet(h);
//			HUtil.addGet(h);
//			HUtil.addGet(h);
//			HUtil.addGet(h);
//			HUtil.addGet(h);
			HUtil.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
