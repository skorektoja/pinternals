package com.pinternals.diffo;

import java.net.HttpURLConnection;
import java.net.URL;

public class TestHUtil {
	public static void main(String[] args) {
		try {
			HUtil hu = new HUtil(10);
			URL u = new URL("http://www.omk.ru");
			HttpURLConnection h = DUtil.getHttpConnection(null, u, 1000);
//			HUtil.addGet(h);
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
