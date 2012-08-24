package com.differencer.pi;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.eclipse.compare.structuremergeviewer.IStructureComparator;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.preferences.IPreferencesService;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import com.differencer.pi.editors.Server;
import com.differencer.pi.nodes.ConfigurationNode;
import com.differencer.pi.preferences.PreferenceConstants;
import com.pinternals.diffo.api.DifferencerFactory;
import com.pinternals.diffo.api.IDiffo;
public class Differencer {
	private static IDiffo diffo = null;
	private static HashMap<Server, ConfigurationNode> structures = new HashMap<Server, ConfigurationNode>();
	public static boolean initDatabase() {
		try {
			return (!diffo.opendb() || !diffo.createdb());
		} catch (ClassNotFoundException e) {
			Activator.log(Status.ERROR, "initDatabase failed", e);
		} catch (SQLException e) {
			Activator.log(Status.ERROR, "initDatabase failed", e);
		}
		return false;
	}
	public static void openDatabase() {
		IPreferencesService service = Platform.getPreferencesService();
		String directory = service.getString(Activator.PLUGIN_ID, PreferenceConstants.P_DATABASE_PATH, "not found database directory preference!", null);
		String file = service.getString(Activator.PLUGIN_ID, PreferenceConstants.P_DATABASE_FILE, "not found database file preference!", null);
		String database = directory + System.getProperty("file.separator") + file;
		Activator.log(Status.INFO, "Using Difference Database: " + database);
		diffo = DifferencerFactory.getDiffo(database, null);
		try {
			if (diffo.opendb() && (diffo.isDbExist() || diffo.createdb())) {
				if (!diffo.start_session()) Activator.log(Status.ERROR, "session not started!");
			}
		} catch (SQLException e) {
			Activator.log(Status.ERROR, "openDatabase failed", e);
		} catch (ClassNotFoundException e) {
			Activator.log(Status.ERROR, "openDatabase failed", e);
		}
	}
	public static void closeDatabase() {
		try {
			if (diffo != null) {
				diffo.finish_session();
				diffo.closedb();
				DifferencerFactory.remove(diffo);
				diffo = null;
				Activator.log(Status.INFO, "closeDatabase success");
			}
		} catch (SQLException e) {
			Activator.log(Status.ERROR, "closeDatabase failed", e);
		}
	}
	public static boolean isConnectedDatabase(){
		return diffo != null ? true : false;
	}
	public static void collectConfiguration(Server description) {
		try {
			diffo.refresh(description.getSID(), description.getURL(), description.getUSER(), description.getPASSWORD());
			structures.remove(description);
		} catch (SQLException e) {
			Activator.log(Status.ERROR, "collectConfiguration failed", e);
		} catch (IOException e) {
			Activator.log(Status.ERROR, "collectConfiguration failed", e);
		} catch (SAXException e) {
			Activator.log(Status.ERROR, "collectConfiguration failed", e);
		} catch (ParseException e) {
			Activator.log(Status.ERROR, "collectConfiguration failed", e);
		}
	}
	public static Object getTree(Server description) {
		return getStructure(description);
	}
	public static void exportXML(Server description, OutputStream os) {
		try {
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			docFactory.setNamespaceAware(true);
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			Document document = docBuilder.newDocument();
			ConfigurationNode structure = (ConfigurationNode) getStructure(description);
			Element root = document.createElement("root");
			root.setAttribute("name", structure.getName());
			document.appendChild(root);
			addChidren(document, root, structure);
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(document);
			StreamResult result = new StreamResult(os);
			transformer.transform(source, result);
		} catch (TransformerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DOMException e) {
			Activator.log(Status.ERROR, "exportXML failed", e);
		} catch (ParserConfigurationException e) {
			Activator.log(Status.ERROR, "exportXML failed", e);
		}
	}
	public static void addChidren(Document document, Element root, ConfigurationNode element) {
		Element j = document.createElement(element.getElementName());
		j.setAttribute("name", element.getName());
		//		
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true);
		Document payload = null;
		try {
			DocumentBuilder docBuilder;
			docBuilder = docFactory.newDocumentBuilder();
			payload = docBuilder.parse(element.getContents());
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		NodeList nodeList = payload.getDocumentElement().getChildNodes();
		for (int i = 0; i < nodeList.getLength(); i++) {
			j.appendChild(document.adoptNode(nodeList.item(i).cloneNode(true)));
		}
		//		
		root.appendChild(j);
		for (Object i : element.getChildren()) {
			if (!i.toString().startsWith("ARIS")) addChidren(document, j, (ConfigurationNode) i);
		}
	}
	public static void exportXMLStream(Server description, OutputStream os) {
		try {
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			docFactory.setNamespaceAware(true);
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			Document document = docBuilder.newDocument();
			ConfigurationNode structure = (ConfigurationNode) getStructure(description);
			Element root = document.createElement(structure.getElementName());
			root.setAttribute("name", structure.getName());
			document.appendChild(root);
			try {
				os.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>".getBytes());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			addChidrenStream(document, root, structure, os);
			try {
				os.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (DOMException e) {
			Activator.log(Status.ERROR, "exportXML failed", e);
		} catch (ParserConfigurationException e) {
			Activator.log(Status.ERROR, "exportXML failed", e);
		}
	}
	private static void addChidrenStream(Document document, Element root, ConfigurationNode element, OutputStream os) {
		Element j = document.createElement(element.getElementName());
		j.setAttribute("name", element.getName());
		//		
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true);
		Document payload = null;
		try {
			DocumentBuilder docBuilder;
			docBuilder = docFactory.newDocumentBuilder();
			payload = docBuilder.parse(element.getContents());
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		NodeList nodeList = payload.getDocumentElement().getChildNodes();
		for (int i = 0; i < nodeList.getLength(); i++) {
			j.appendChild(document.adoptNode(nodeList.item(i).cloneNode(true)));
		}
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = null;
		try {
			transformer = transformerFactory.newTransformer();
		} catch (TransformerConfigurationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		DOMSource source = new DOMSource(j);
		StreamResult result = new StreamResult(os);
		try {
			transformer.transform(source, result);
		} catch (TransformerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//		
		// root.appendChild(j);
		for (Object i : element.getChildren()) {
			addChidrenStream(document, j, (ConfigurationNode) i, os);
		}
	}
	public static void reloadStructure(Server description) {
		structures.remove(description);
		getStructure(description);
	}
	public static IStructureComparator getStructure(Server description) {
		if (structures.containsKey(description)) return structures.get(description);
		ConfigurationNode root = new ConfigurationNode(description, DifferencerFactory.getDifferencerNode(diffo, description.getSID(), description.getURL(), description.getUSER(), description.getPASSWORD()));
		structures.put(description, root);
		description.fireEvent();
		return root;
	}
	public static IStatus collectConfigurationForNode(ConfigurationNode node, IProgressMonitor monitor) {
		try {
			node.getNode().refresh(diffo, false);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Object[] children = node.getChildren();
		for (int i = 0; i < children.length; i++) {
			if (monitor != null) monitor.subTask("Collect data for " + children[i].toString());
			IStatus status = ((ConfigurationNode) children[i]).refresh(monitor);
			if (monitor != null) monitor.worked(1);
			if (monitor != null) if (monitor.isCanceled()) {
				Activator.log(Status.WARNING, "Configuration collection cancelled");
				return Status.CANCEL_STATUS;
			}
		}
		return Status.OK_STATUS;
	}
}
