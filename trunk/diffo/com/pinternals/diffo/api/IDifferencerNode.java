package com.pinternals.diffo.api;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.text.ParseException;

import org.xml.sax.SAXException;

public interface IDifferencerNode {
	public String getName(); //full name of the object -> toString() for display in Eclipse tree
	public String getVersion(); //unique identifier of the object instance
	public String getElementName(); //XML name (full object name may contains invalid symbols: | : etc.)
	public InputStream getPayload(); //stream for editors
	public void setParent(Object p); // TODO possible candidate for private
	public Object getParent(); //need for Eclipse tree show, blin...
	public void addChild(IDifferencerNode c); // TODO possible candidate for private
	public Object[] getChildren(); //major function to create structure

	public String getObjectSWCV(); //for transport
	public String getObjectID(); //for transport
	public String getObjectName(); //for transport
	public String getObjectType(); //for transport
	public String getObjectNamespace(); //for transport
	public String getTransportSuffix(); //for transport: /rep/ or /dir/
	public boolean equals(Object other); //for object comparison operations
	public int hashCode(); //for structure comparison operations 
	public String toString(); //need for Eclipse tree show, blin...
	public boolean refresh(IDiffo diffo, boolean withChildren)
	throws MalformedURLException, SQLException, IOException, SAXException, ParseException, InterruptedException;
}
