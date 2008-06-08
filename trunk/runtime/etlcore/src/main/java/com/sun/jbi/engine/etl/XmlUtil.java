/*
 * BEGIN_HEADER - DO NOT EDIT
 * 
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the "License").  You may not use this file except
 * in compliance with the License.
 *
 * You can obtain a copy of the license at
 * https://open-jbi-components.dev.java.net/public/CDDLv1.0.html.
 * See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * HEADER in each file and include the License file at
 * https://open-jbi-components.dev.java.net/public/CDDLv1.0.html.
 * If applicable add the following below this CDDL HEADER,
 * with the fields enclosed by brackets "[]" replaced with
 * your own identifying information: Portions Copyright
 * [year] [name of copyright owner]
 */

/*
 * @(#)XmlUtil.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.logging.Logger;
import java.util.logging.Level;
import com.sun.jbi.engine.etl.Localizer;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.sun.sql.framework.utils.RuntimeAttribute;

/**
 * Description of the Class
 *
 * @author       Bing Lu
 * @created      May 5, 2005
 */
public class XmlUtil {
	private static transient final Logger mLogger = Logger.getLogger(XmlUtil.class.getName());

    private static transient final Localizer mLoc = Localizer.get();
    
    static DocumentBuilder mBuilder = null;
    
    /**
     * Description of the Method
     *
     * @param namespaceAware  Description of the Parameter
     * @return                Description of the Return Value
     * @exception Exception   Description of the Exception
     */
    public static Document createDocument(boolean namespaceAware)
        throws Exception {

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        factory.setNamespaceAware(namespaceAware);

        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.newDocument();

        return document;
    }
    
    /**
     * Description of the Method
     *
     * @param namespaceAware  Description of the Parameter
     * @param source          Description of the Parameter
     * @return                Description of the Return Value
     * @exception Exception   Description of the Exception
     */
    private static Document createDocument(boolean namespaceAware,
            InputSource source)
             throws Exception {

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        factory.setNamespaceAware(true);
        //factory.setValidating();

        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(source);

        document.normalize();

        return document;
    }

    /**
     * Description of the Method
     *
     * @param namespaceAware  Description of the Parameter
     * @param xml             Description of the Parameter
     * @return                Description of the Return Value
     * @exception Exception   Description of the Exception
     */
    public static Document createDocumentFromXML(boolean namespaceAware,
            String xml)
             throws Exception {
        return createDocument(namespaceAware,
                new InputSource(new StringReader(xml)));
    }

    /**
     * Gets the text attribute of the DOMUtil class
     *
     * @param node  Description of the Parameter
     * @return      The text value
     */
    public static String getText(Node node) {
        StringBuffer buf = new StringBuffer();
        NodeList children = node.getChildNodes();
        for (int i = 0, I = children.getLength(); i < I; i++) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.TEXT_NODE) {
                buf.append(child.getNodeValue());
            }
        }
        return buf.toString();
    }

    /**
     * Description of the Method
     *
     * @param node  Description of the Parameter
     * @return      Description of the Return Value
     */
    // UTF-8
    public static String toXml(Node node, String encoding, boolean omitXMLDeclaration) {
        String ret = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            Transformer trans = TransformerFactory.newInstance().newTransformer();
            trans.setOutputProperty(OutputKeys.ENCODING, encoding);
            trans.setOutputProperty(OutputKeys.INDENT, "yes");
            trans.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
            trans.setOutputProperty(OutputKeys.METHOD, "xml");
            trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, omitXMLDeclaration? "yes":"no");
            trans.transform(new DOMSource(node), new StreamResult(baos));
            ret = baos.toString(encoding);
			mLogger.log(Level.INFO,mLoc.loc("INFO071: ret: {0} ",ret));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

	public static void showNodes(Element e, int level) {
		NodeList nodeList = e.getChildNodes();
	
		for (int i = 0; i < nodeList.getLength(); i++) {
			Node node = nodeList.item(i);
		mLogger.log(Level.INFO,mLoc.loc("INFO070: Node[{0}]: {1} ",i,node));
			if (node instanceof Element) {
				showNodes((Element) node, level + 1);
			}
		}
	}

	public static  Object convertInputArgument(RuntimeAttribute ra, String value) {
		Object result = null;
	
		switch (ra.getJdbcType()) {
	
		case Types.BOOLEAN:
			result = Boolean.valueOf(value);
			break;
		case Types.INTEGER:
			result = Integer.valueOf(value);
			break;
		case Types.DECIMAL:
			result = BigDecimal.valueOf(Double.valueOf(value).doubleValue());
			break;
		case Types.DOUBLE:
			result = Double.valueOf(value);
			break;
		case Types.FLOAT:
			result = Float.valueOf(value);
			break;
		case Types.DATE:
			result = Date.valueOf(value);
		case Types.TIME:
			result = Time.valueOf(value);
		case Types.TIMESTAMP:
			result = Timestamp.valueOf(value);
			break;
		case Types.CHAR:
		case Types.VARCHAR:
			result = value;
			break;
	
		default:
			result = value;
	
		}
		return result;
	}

	public static Document newDocument() throws ParserConfigurationException {
		if (mBuilder == null) {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			mBuilder = factory.newDocumentBuilder();
		}
	
		return mBuilder.newDocument();
	}

}
