package com.pinternals.diffo;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.xml.sax.SAXException;

import com.pinternals.diffo.PiObject.Kind;

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
	class Zz implements Runnable{
		boolean del=false, online=false, error = false;
		Exception e;
		List<PiObject> lst;
		Zz(boolean dl, boolean onl) {
			del=dl;
			online=onl;
		}
		@Override
		public void run() {
			try {
				if (online)
					lst = ent.host.askIndexOnline(ent, del);
				else
					lst = ent.host.diffo.__getIndexDb(ent.host, ent);
			} catch (Exception e) {
				error = true;
				this.e = e;
			}
		}
	}
	class Zy implements Callable<List<PiObject>>{
		boolean error = false;
		Exception e = null;
		@Override
		public List<PiObject> call() {
			Zz del = new Zz(true, true), act = new Zz(false, true), db = new Zz(true,false);
			Thread t1 = new Thread(del);
			Thread t2 = new Thread(act);
			Thread t3 = new Thread(db);
			t1.start();
			t2.start();
			t3.start();
			try {
				t1.join();
				t2.join();
				t3.join();
			} catch (Exception ex) {
				error = true;
				this.e = ex;
			} 
			error = error || del.error || act.error || db.error;
			this.e = this.e!=null ? this.e :
				del.error ? del.e :
					act.error ? act.e : 
						db.error ? db.e : null;
			List<PiObject> objs2 = null;
			if (!error) {
				List<PiObject> tmp = act.lst;
				tmp.addAll(del.lst);
				act = null; 
				del = null;
				objs2 = ent.host.diffo.mergeObjects(ent.host, ent, db.lst, tmp);
				try {
					ent.addUpdateQueue(objs2);
					ent.host.diffo.loopUpdateQueue(ent.updateQueue);
				} catch (Exception sqle) {
					error = true;
					this.e = sqle;
				}
			}
			return objs2; 
		}
	}
	
	Callable<List<PiObject>> getObjectsIndex(boolean bg) throws IOException, SAXException, SQLException, InterruptedException, ExecutionException {
		assert side!=null && ent!=null : "Either side or entity are empty";
		final PiHost p = ent.host;
		final Diffo d = side.root.d;
		if (side.side==Side.Repository && ent.intname.equals("workspace")) {
			objs = new ArrayList<PiObject>(100);
			for (SWCV s: p.swcv.values()) objs.add(s);
		} else if (bg && ent.is_indexed)  {
			return new Zy();
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
		return null;
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


