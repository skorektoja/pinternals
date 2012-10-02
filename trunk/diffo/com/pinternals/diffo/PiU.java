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
	void getObjectsIndex() throws IOException, SAXException, InterruptedException, ExecutionException, SQLException {
		assert side!=null && ent!=null : "Either side or entity are empty";
		PiHost p = ent.host;
		Diffo d = side.root.d;
		if (side.side==Side.Repository && ent.intname.equals("workspace")) {
			objs = new ArrayList<PiObject>(100);
			for (SWCV s: p.swcv.values()) objs.add(s);
		} else if (side.side==Side.Directory && (
				ent.intname.equals("AgencySchemObj") 
				|| false
				) ) { 
			// ignore
		} else if (side.side==Side.Repository && ( 
				ent.intname.equals("ifmopmess") 
				|| ent.intname.equals("MAP_HELPER")
				) ) {
			// ignore
		} else if ( (side.side==Side.Directory && ent.intname.equals("MappingRelation")) 
				|| (side.side==Side.Repository && ent.intname.equals("XI_TRAFO"))
				) {
			List<PiObject> act = p.askIndexOnline(ent, false), del = p.askIndexOnline(ent, true);
			act.addAll(del);
			del = null;
			List<PiObject> db = d.__getIndexDb(p, ent);
			objs = d.mergeObjects(p, ent, db, act);
			// ignore
		} else {
			List<PiObject> act = p.askIndexOnline(ent, false), del = p.askIndexOnline(ent, true);
			act.addAll(del);
			del = null;
			List<PiObject> db = d.__getIndexDb(p, ent);
			objs = d.mergeObjects(p, ent, db, act);
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


