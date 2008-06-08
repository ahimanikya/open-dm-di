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
 * @(#)XMLDocumentUtils.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.mbeans;

import java.util.Vector;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class XMLDocumentUtils {

	public static String getAttribute(Element xmlElement, String attr_collab_name, boolean b) {
		return xmlElement.getAttribute(attr_collab_name);

	}

	public static Vector getChildren(Element xmlElement, String elem_inbound_connector) {
		NodeList nl =  xmlElement.getElementsByTagName(elem_inbound_connector);
		Vector v = new Vector();
		for (int i = 0; i < nl.getLength(); i++) {
			
			v.add(nl.item(i).getNodeValue());
		} 
		return v;
	}

}
