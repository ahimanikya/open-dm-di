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
 * @(#)DataColumn.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package org.axiondb.service.client;

import java.io.Serializable;

/**
 * @author Ritesh Adval
 */
public class DataColumn implements Serializable {

    public DataColumn(String val, int position) {
        _value = val;
        _position = position;
    }

    public int getPosition() {
        return _position;
    }

    public String getValue() {
        return _value;
    }

    public void setPosition(int position) {
        _position = position;
    }

    public void setValue(String val) {
        _value = val;
    }

    private int _position;
    private String _value;
}
