package com.pinternals.diffo;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;

import org.xml.sax.SAXException;

class HierRoot{
	int level;
	PiHost p = null;
	Diffo d = null;
	List<HierSide> children = new LinkedList<HierSide>();
	HierRoot(){
		level = 0;
	}
	protected HierSide addSide(Side s) {
		return new HierSide(this, s);
	}
}
class HierSide extends HierRoot {
	List<HierEnt> children = new LinkedList<HierEnt>();
	HierRoot root;
	Side side;
	HierSide(){}
	HierSide(HierRoot r, Side s) {
		level = 1;
		side = s;
		root = r;
		r.children.add(this);
	}
	protected HierEnt addPiEntity(PiEntity e) {
		return new HierEnt(this, e);
	}
}
class HierEnt extends HierSide {
	HierSide side;
	PiEntity ent;
	HierEnt(){}
	HierEnt(HierSide s, PiEntity e) {
		level = 2;
		side = s;
		ent = e;
		side.children.add(this);
	}
	void getObjectsIndex() throws IOException, ParseException, SQLException, SAXException, InterruptedException {
		if (side.side==Side.Repository && ent.intname.equals("workspace")) {
//			d.__refreshSWCV(p, ent);

		}
	}
}
class HierObj extends HierEnt {
	HierEnt ent;
	Object obj;
	HierObj(){}
	HierObj(HierEnt e, Object o) {
		level = 3;
		ent = e;
		obj = o;
		ent.children.add(this);
	}
}



public class PiU implements HTaskListener, Runnable {
	@Override
	public void finished(HTask h) {
		
	}

	@Override
	public void run() {
		
	}
}


