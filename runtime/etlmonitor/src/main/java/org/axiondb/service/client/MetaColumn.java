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
 * @(#)MetaColumn.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package org.axiondb.service.client;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Ritesh Adval
 */
public class MetaColumn implements Serializable {

    public MetaColumn(int cPosition) {
        _position = cPosition;
    }

    public String getAttr(String name) {
        return (String) _attrs.get(name);
    }

    public Map getAttributes() {
        return _attrs;
    }

    public int getPosition() {
        return _position;
    }

    public void putAttr(String name, String value) {

        Attr attr = (Attr) _attrs.get(name);
        if (attr == null) {
            attr = new Attr(name, value);
            _attrs.put(name, attr);
        } else {
            attr.setValue(value);
        }
    }

    public void setPosition(int cPosition) {
        _position = cPosition;
    }

    public static final String NAME = "NAME";

    private Map _attrs = new HashMap(5);
    private int _position;
}
