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
 * @(#)PortMapReader.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl;

import java.util.Iterator;
import java.util.LinkedList;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import com.sun.jbi.management.descriptor.SUDescriptorSupport;
import com.sun.jbi.management.descriptor.Provides;

public class PortMapReader {
    private static final String PORTMAP_TAG = "portmap";
    private static final String SERVICENAME_TAG = "service";
    private static final String ENDPOINT_TAG = "endPoint";
    private static final String ROLE_TAG = "role";
    private static final String PARTNERLINK_TAG = "partnerLink";

    /** Creates a new instance of PortMapReader */
    protected PortMapReader() {
    }

    public static Iterator parse(java.io.File portmapfile)
        throws org.xml.sax.SAXException,
               java.io.IOException,
               javax.xml.parsers.ParserConfigurationException {

        LinkedList entries = null;

        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(portmapfile);
        Element elem = doc.getDocumentElement();
        NodeList portmaps = elem.getElementsByTagName(PORTMAP_TAG);
        for (int i = 0; i < portmaps.getLength(); i++) {
            NamedNodeMap values = portmaps.item(i).getAttributes();

            QName service = getQName(values.getNamedItem(SERVICENAME_TAG).getNodeValue());
            QName endpoint = getQName(values.getNamedItem(ENDPOINT_TAG).getNodeValue());
            String role = values.getNamedItem(ROLE_TAG).getNodeValue();
            QName partnerlink = getQName(values.getNamedItem(PARTNERLINK_TAG).getNodeValue());

            if (entries == null) {
                entries = new LinkedList();
            }

            PortMapEntry entry = new PortMapEntry(service, endpoint, role, partnerlink);
            entries.add(entry);
        }
        return (entries != null) ? entries.iterator() : null;
    }

    public static Iterator parse(SUDescriptorSupport sud) {
        LinkedList entries = null;

        Provides[] pds = sud.getProvides();
        for (int i=0; i<pds.length; i++) {
            //mLogger.info("DTEL Provide[" + i + " ]: " + pds[i].getServiceName()+", "+pds[i].getEndpointName());
            Provides p = pds[i];

            QName service = p.getServiceName();
            QName endpoint = new QName(p.getEndpointName());
            String role = "myRole";
            QName partnerlink = p.getServiceName();

            if (entries == null) {
                entries = new LinkedList();
            }

            PortMapEntry entry = new PortMapEntry(service, endpoint, role, partnerlink);
            entries.add(entry);
        }

        return (entries != null) ? entries.iterator() : null;
    }

    private static QName getQName(String qname) {
        return QName.valueOf(qname);
    }
}
