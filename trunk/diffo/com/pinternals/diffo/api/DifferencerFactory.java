package com.pinternals.diffo.api;

import java.io.IOException;
import java.net.Proxy;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.xml.sax.SAXException;

import com.pinternals.diffo.DiffItem;
import com.pinternals.diffo.Diffo;
import com.pinternals.diffo.PiEntity;
import com.pinternals.diffo.PiHost;
import com.pinternals.diffo.Side;
import com.pinternals.diffo.impl.DifferencerNode;

public class DifferencerFactory {
	private static HashMap<IDiffo, Diffo> hm = new HashMap<IDiffo, Diffo>(2);
	
	public static void remove(IDiffo i) {
		hm.remove(i);
	}
	
	public static IDiffo getDiffo(String dbFilePath, Proxy prx, int tx) {
		Diffo d = new Diffo(dbFilePath, prx, tx);
		hm.put(d, d);
		return d;
	}

//	public static Diffo getDiffo(IDiffo idf) {
//		return hm.get(idf);
//	}

	public static IDifferencerNode getDifferencerNode(IDiffo idiffo, String sid, String url, String user, String password) {
		DifferencerNode root = new DifferencerNode("root");
		Diffo diffo = hm.get(idiffo);
		assert diffo != null;
		try {
			PiHost pihost = diffo.addPiHost(sid, url);
			pihost.setUserCredentials(user, password);
			diffo.refreshMeta(pihost);
			Side[] sides = { Side.Repository, Side.Directory, Side.SLD };
			for (Side side : sides) {
				DifferencerNode side_root = new DifferencerNode(side.toString());
				root.addChild(side_root);
				for (PiEntity entity : pihost.entities.values()) {
					if (!side.equals(entity.side)) continue;
					DifferencerNode side_entity = new DifferencerNode(entity.title);
					side_root.addChild(side_entity);
					ArrayList<DiffItem> items = diffo.list(pihost, entity.side, entity.intname);
					Iterator<DiffItem> il = items.iterator();
					while (il.hasNext()) {
						side_entity.addChild(new DifferencerNode(il.next()));
					}
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return root;
	}
}
