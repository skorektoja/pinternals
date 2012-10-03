package com.pinternals.diffo;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.xml.sax.SAXException;

interface Hier {
}
class HierRoot implements Hier {
	List<HierSide> sides = new LinkedList<HierSide>();
	PiHost p = null;
	Diffo d = null;
	HierRoot(Diffo d, PiHost p){
		this.d = d;
		this.p = p;
	}
	protected HierSide addSide(Side s) {
		HierSide hs = new HierSide(this, s); 
		sides.add(hs);
		return hs;
	}
}
class HierSide implements Hier {
	List<HierEnt> ents = new LinkedList<HierEnt>();
	HierRoot root;
	Side side;
	HierSide(HierRoot r, Side s) {
		side = s;
		root = r;
	}
	protected HierEnt addPiEntity(PiEntity e) {
		HierEnt h = new HierEnt(this, e);
		ents.add(h);
		return h;
	}
}
class HierEnt implements Hier {
	List<PiObject> objs = null;
	HierSide side;
	PiEntity ent;
	HierEnt(HierSide s, PiEntity e) {
		side = s;
		ent = e;
	}
	void getObjectsIndex() throws IOException, SAXException, SQLException {
		assert side!=null && ent!=null : "Either side or entity are empty";
		boolean bg = false;
		final PiHost p = ent.host;
		final Diffo d = side.root.d;
		if (side.side==Side.Repository && ent.intname.equals("workspace")) {
			objs = new ArrayList<PiObject>(100);
			for (SWCV s: p.swcv.values()) objs.add(s);
		} else if (bg && ent.is_indexed)  {
			Thread t = new Thread(new Runnable(){
				@Override
				public void run() {
					List<PiObject> act=null, del=null, db=null;
					try {
						act = p.askIndexOnline(ent, false);
						del = p.askIndexOnline(ent, true);
						db = d.__getIndexDb(p, ent);
						act.addAll(del);
						objs = d.mergeObjects(p, ent, db, act);
						ent.addUpdateQueue(objs);
						ent.host.diffo.loopUpdateQueue(ent.updateQueue);
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
					}
				}
			});
			t.start();
		} else if (ent.is_indexed) {
			List<PiObject> act = p.askIndexOnline(ent, false), 
					del = p.askIndexOnline(ent, true);
			act.addAll(del);
			del = null;
			List<PiObject> db = d.__getIndexDb(p, ent);
			objs = d.mergeObjects(p, ent, db, act);
			ent.addUpdateQueue(objs);
			ent.host.diffo.loopUpdateQueue(ent.updateQueue);
		}
	}
}
class HierObj implements Hier {
	HierEnt ent;
}
public class PiU implements Callable<PiObject> {
	@Override
	public PiObject call() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}


