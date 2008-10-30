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
 * SeletableFilter.java
 * 
 * Created on Sep 26, 2007, 3:37:53 PM
 * 
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.dimi.qmquery;

import com.sun.mdm.index.util.Logger;
import java.util.ArrayList;

/**
 *
 * @author Manish Bharani
 */
public class SelectableFilter {
    
    ArrayList seletables = null;
    
    /**
     * logger
     */
    private static Logger sLog = Logger.getLogger(SelectableFilter.class.getName());    

    public SelectableFilter() {
        seletables = new ArrayList();
    }
    
    public void addSeletable(String qualifiedPath){
        seletables.add(qualifiedPath);
    }
    
    public void addSeletable(String[] filterlist){
        for (int i=0; i < filterlist.length; i++){
            seletables.add(filterlist[i]);
        }
    }
    
    public ArrayList getAddedSelectables(){
        return seletables;
    }
}
