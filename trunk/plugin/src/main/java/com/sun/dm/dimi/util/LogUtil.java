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
package com.sun.dm.dimi.util;

import java.util.HashMap;
import com.sun.mdm.index.util.PackageTree;
import net.java.hulp.i18n.Logger;

/**
 * Log utility functions customized for DataQuality
 * This class creates a mapping from packages to Component name
 * and gives a jdk logger instance given a class name
 * @author srengara@dev.java.net
 */
public class LogUtil {

    private static PackageTree sLogMapping;
    static {

        try {
            //Construct a package tree with default string "Common"
            sLogMapping = new PackageTree("Common");

            //Construct hashmap used to load tree
            HashMap hm = new HashMap();
                        
            //eTL-eView Plugin Packages
            String[] pluginPackages = new String[]{
                "com.sun.dm.dimi.dataobject.metadata",
                "com.sun.dm.dimi.datareader",
                "com.sun.dm.dimi.datawriter",
                "com.sun.dm.dimi.plugin",
                "com.sun.dm.dimi.qmquery",
                "com.sun.dm.dimi.util",
                "com.sun.dm.dimi"
            };
            hm.put("Plugin", pluginPackages);
            
            sLogMapping.addAssignment(hm);
        } catch (Exception e) {
            //Logger.getLogger(LogUtil.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    /** Logger prefix */
    public static final String DATAINTEGRATION_LOG_PREFIX = "SUN.MDM.DI.";

    
    /**
     * This method returns an instance of <code>java.util.Logger</code> given a class name
     * @param className
     * @return logger instance
     **/
    public static Logger getLogger(String className) {
        return getLogger(LogUtil.DATAINTEGRATION_LOG_PREFIX,className);
    }
    
    /** Get logger for given object and prefix
     * @param logPrefix
     * @param className class name
     * @return logger
     */
    public static Logger getLogger(String logPrefix, String className) {
        if(logPrefix == null || "".equalsIgnoreCase(logPrefix)) {
            logPrefix = LogUtil.DATAINTEGRATION_LOG_PREFIX;
        }
        String componentName = (String) sLogMapping.getObjectValue(className);
        return Logger.getLogger(logPrefix + "." + componentName + "." + className);
    }



    public static void main(String[] args) {
        String[] testClasses = {"com.sun.dm.dimi.datareader", "com.sun.dm.dimi.datareader", "com.sun.dm.dimi.util"};
        for (int i = 0; i < testClasses.length; i++) {
            System.out.println(sLogMapping.getObjectValue(testClasses[i]));
        }
    }
}
