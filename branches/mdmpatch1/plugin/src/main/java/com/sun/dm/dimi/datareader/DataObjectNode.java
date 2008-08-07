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

import com.sun.mdm.index.objects.ObjectNode;
import java.util.HashMap;

/**
 *
 * @author Manish
 */
public class DataObjectNode {
    
    private String[] defaultFieldsValue = null;
    private ObjectNode on = null;
    private int extFldLen = DefaultSystemFields.getDefaultSystemFields().length;
    private String[] defaultFieldsNames = DefaultSystemFields.getDefaultSystemFields();
    
    public DataObjectNode(){
        defaultFieldsValue = new String[extFldLen];
    }
    
    public void setSystemFieldValue(int index, String value ){
        defaultFieldsValue[index] = value;
    }
    
    public String[] getDefaultSystemFields(){
        return defaultFieldsValue;
    }
    
    public void setObjectNode(ObjectNode objectnode){
        this.on = objectnode;
    }
    
    public ObjectNode getObjectNode(){
       return this.on;
    }
    
    public boolean isAttributeDefault(String attrib) {
        for (int i=0; i < extFldLen; i++){
            if (defaultFieldsNames[i].equals(attrib)){
                return true;
            }
        }
        return false;
    }
    
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Default System Fields : ");
        for (int i=0; i<extFldLen; i++){
            sb.append("\n     " + defaultFieldsValue[i]);
        }
        sb.append(on.toString());
        return sb.toString();
    }
    
    

}
