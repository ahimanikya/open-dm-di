/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2003-2007 Sun Microsystems, Inc. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the Common 
 * Development and Distribution License ("CDDL")(the "License"). You 
 * may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the License at
 * https://open-dm-mi.dev.java.net/cddl.html
 * or open-dm-mi/bootstrap/legal/license.txt. See the License for the 
 * specific language governing permissions and limitations under the  
 * License.  
 *
 * When distributing the Covered Code, include this CDDL Header Notice 
 * in each file and include the License file at
 * open-dm-mi/bootstrap/legal/license.txt.
 * If applicable, add the following below this CDDL Header, with the 
 * fields enclosed by brackets [] replaced by your own identifying 
 * information: "Portions Copyrighted [year] [name of copyright owner]"
 */

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sun.dm.dimi.datareader;

import com.sun.dm.dimi.util.Localizer;
import com.sun.dm.dimi.util.LogUtil;
import java.util.HashMap;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish
 */
public class DefaultSystemFields {

    // Enter System fields that need to be set as defaults (UPPERCASE)
    // These fields must be present in the source system from where data is being pulled out.
     private static final String[] defaults = {
            "GID",
            "SYSTEMCODE",
            "LID",
            "UPDATEDATE",
            "USR"
     };
    
    //logger
    private static Logger sLog = LogUtil.getLogger(DefaultSystemFields.class.getName());
    private static Localizer sLoc = Localizer.get();
    HashMap fieldMap = new HashMap<String, Integer>();

    public DefaultSystemFields() {
        sLog.fine("Default System Fields ..." + printfields());
        createFieldMap();
    }

    private String printfields() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < defaults.length; i++) {
            sb.append("\n   " + defaults[i]);
        }
        return sb.toString();
    }

    // This is to speed up search of fields at runtime
    private void createFieldMap() {
        for (int i = 0; i < defaults.length; i++) {
            fieldMap.put(defaults[i], new Integer(i));
        }
    }

    // If Attribute is found, index is returned for the attribute
    public int isAttributeDefault(String attrib) {
        Integer index = (Integer) fieldMap.get(attrib);
        if (index != null) {
            return index.intValue();
        }
        return -1;
    }
    
    public static String[] getDefaultSystemFields(){
        return defaults;
    }
}
