package com.differencer.pi.editors;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.eclipse.core.resources.IStorage;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IPath;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import com.differencer.pi.Differencer;
import com.differencer.pi.nodes.ConfigurationNode;
public class ServerStorage implements IStorage {
	private ConfigurationNode element;
	public ServerStorage(ConfigurationNode e) {
		element = e;
	}
	@Override
	public InputStream getContents() throws CoreException {
		if(element.toString().startsWith("ARIS")) return new ByteArrayInputStream("<?xml version=\"1.0\" encoding=\"UTF-8\"?><empty />".getBytes());
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true);
		DocumentBuilder docBuilder = null;
		try {
			docBuilder = docFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Document document = docBuilder.newDocument();
		Element root = document.createElement("root");
		root.setAttribute("name", element.getName());
		document.appendChild(root);
		Differencer.addChidren(document, root, element);
		DOMSource source = new DOMSource(document);
		StringWriter xmlAsWriter = new StringWriter();
		StreamResult result = new StreamResult(xmlAsWriter);
		try {
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			transformerFactory.setAttribute("indent-number", 2);
			Transformer transformer = transformerFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.transform(source, result);
			return new ByteArrayInputStream(xmlAsWriter.toString().getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransformerConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransformerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransformerFactoryConfigurationError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	@Override
	public IPath getFullPath() {
		return null;
	}
	@Override
	public String getName() {
		return element.getName();
	}
	@Override
	public boolean isReadOnly() {
		return true;
	}
	@Override
	public Object getAdapter(Class adapter) {
		return null;
	}
}
