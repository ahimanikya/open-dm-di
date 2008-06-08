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
 * @(#)ETLPersistableMessageImpl.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.sun.sql.framework.utils.StringUtil;
import com.sun.sql.framework.utils.XmlUtil;

/**
 * This class implements the ETLPersistableMessage
 * 
 */

@SuppressWarnings("unchecked")
public class ETLPersistableMessageImpl implements ETLPersistableMessage {

    /**
     * Increment this version number whenever a significant change is made to the serialized
     * version of this message. 
     */
    private static final int CURRENT_VERSION = 1;

    private static final String ELEMENT_PART = "part";
    private static final String ATTR_NAME = "name";
    private static final String ATTR_VALUE = "value";
    
    private String etlWSMessage;
    private Map parts = new HashMap();
    private int version = CURRENT_VERSION;

    /**
     * Setter for Parts
     * 
     * @param theParts for the container
     */
    
	public void addPart(String partName, Object partValue) {
        this.parts.put(partName, partValue);
    }

    public Object getPart(String partName) {
        return this.parts.get(partName);
    }

    /**
     * getter for Parts
     * 
     * @return theParts for the container
     */
    public Map getParts() {
        return this.parts;
    }

    /**
     * @see com.stc.etl.runtime.ETLPersistableMessage#persist(java.io.DataOutputStream)
     */
    public void persist(DataOutputStream dos) throws Exception {
        switch (version) {
            case 1:
                dos.writeInt(version);
                etlWSMessage = toXMLString();
                dos.write(etlWSMessage.getBytes("UTF-8"));
                break;
                
            default:
                throw new Exception("Unsupported version: " + version);
        }
    }

    /**
     * @see com.stc.etl.runtime.ETLPersistableMessage#restore(java.io.DataInputStream)
     */
    public void restore(DataInputStream dis) throws Exception {
        int tmpVersion = dis.readInt();
        switch (tmpVersion) {
            case 1:
                byte[] wsMessageBytes = getContentsAsBytes(dis);
                etlWSMessage = new String(wsMessageBytes, "UTF-8");
                setParts(parseXML(etlWSMessage));
                break;
                
            default:
                throw new Exception("Unsupported version: " + tmpVersion);
        }
        version = tmpVersion;
    }

    /**
     * Setter for Parts
     * 
     * @param newParts for the container
     */
    public void setParts(Map newParts) {
        parts.clear();
        parts.putAll(newParts);
    }

    private byte[] getContentsAsBytes(DataInputStream dis) throws IOException {
        byte[] chunk = new byte[1024];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        while (true) {
            int bytesRead = dis.read(chunk);
            if (bytesRead == -1)
                break;
            bos.write(chunk, 0, bytesRead);
        }
        bos.close();

        return bos.toByteArray();
    }

    private String toXMLString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\r\n");
        buffer.append("<eTLWSMessage>\n");
        Iterator iter = parts.keySet().iterator();
        while (iter.hasNext()) {
            Object key = iter.next();
            Object value = parts.get(key);
            
            if (value != null) {
                buffer.append("\t<").append(ELEMENT_PART).append(" ").append(ATTR_NAME).append("=");
                buffer.append("\"").append(key).append("\" ");

                buffer.append(ATTR_VALUE).append("=").append("\"").append(XmlUtil.escapeXML(
                    value.toString().trim())).append("\"");
                buffer.append("/>\n");
            }
        }
        buffer.append("</eTLWSMessage>");
        return buffer.toString();
    }
    

    /**
     * Generates a Map of message parts (key-value pairs) from the given String. 
     * 
     * @param messageXml XML containing message parts to be parsed
     * @return Map of message parts parsed from <code>messageXml</code>
     */
    private Map parseXML(String messageXml) {
        Map msgMap = Collections.EMPTY_MAP;
        
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder db = dbFactory.newDocumentBuilder();
            Document doc = db.parse(new InputSource(new StringReader(messageXml)));
            Element root = doc.getDocumentElement();
            if (root != null) {
                msgMap = parseParts(root.getChildNodes());
            }
        } catch (ParserConfigurationException ignore) {
            ignore.printStackTrace();
        } catch (SAXException ignore) {
            ignore.printStackTrace();
        } catch (IOException ignore) {
            ignore.printStackTrace();
        }        
        
        return msgMap;
    }

    /**
     * Parses the given NodeList to extract key-value String pairs that are associated with 
     * eTL WSMessage Part nodes.
     * 
     * @param childNodes NodeList to parse
     * @return Map of zero or more message part name-value pairs 
     */
    private Map parseParts(NodeList childNodes) {
        Map msgMap = new HashMap(childNodes.getLength());
        
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE && ELEMENT_PART.equals(((Element) node).getNodeName())) {
                Element part = (Element) node;
                String name = part.getAttribute(ATTR_NAME);
                String value = part.getAttribute(ATTR_VALUE);
                if (!StringUtil.isNullString(name) && value != null) {
                    msgMap.put(name, value);
                }
            }
        }
        
        return msgMap;
    }
}
