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
import com.sun.dm.dimi.util.PluginConstants;
import com.sun.mdm.index.dataobject.DataObject;
import com.sun.mdm.index.objects.ObjectField;
import com.sun.mdm.index.objects.ObjectNode;
import com.sun.mdm.index.objects.exception.ObjectException;
import java.util.ArrayList;
import java.util.logging.Level;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish
 */
public class ObjectFinalizerSlave extends Thread {

    BaseDBDataReader basedbreader = null;
    boolean finalizerIsRunning = false;
    //logger
    private static Logger sLog = LogUtil.getLogger(ObjectFinalizerSlave.class.getName());
    private static Localizer sLoc = Localizer.get();

    public ObjectFinalizerSlave() {
    }

    public ObjectFinalizerSlave(BaseDBDataReader basedbreader) {
        this.basedbreader = basedbreader;
    }

    @Override
    public void run() {
        finalizerIsRunning = true;
        sLog.info(sLoc.x("PLG070: Object Finalizer Slave : Starting Finalizer Thread ..."));
        while (finalizerIsRunning) {
            //Check if the queue in base db data reader has any objects to finalize
            if (!basedbreader.usedObjectsList.isEmpty()) {
                // Start finalizing from the first available instance
                Object objectToFinalize = basedbreader.usedObjectsList.remove(0);
                finalizeObject(objectToFinalize);
            } else {
                // Sleep
                goSleep(PluginConstants.nodata_sleep);
            }
        }
        sLog.info(sLoc.x("PLG080: Object Finalizer Slave : Finalizer Thread Stopped"));
    }

    private void finalizeObject(Object finobj) {
        if (finobj instanceof DataObjectNode) {
            //System.out.println("1. Instance of Data Object Node");
            deepCleanDataObjectNode((DataObjectNode) finobj);
        } else if (finobj instanceof DataObject) {
            //System.out.println("2. Instance of Data Object");
            deepCleanDataObject((DataObject) finobj);
        } else {
            sLog.warn(sLoc.x("PLG071: Unidentified Object Type. UNable to Finalize Object."));
        }
    }

    private void deepCleanDataObjectNode(DataObjectNode donode) {
        //Clear System Fileds
        String[] defFields = donode.getDefaultSystemFields();
        for (int i = 0; i < defFields.length; i++) {
            donode.setSystemFieldValue(i, null);
        }
        defFields = null;

        //Clear Object Node
        ObjectNode onode = donode.getObjectNode();
        deepCleanObjectNode(onode);
        onode = null;
        donode = null;

    }

    private void deepCleanDataObject(DataObject dobject) {
        // Removes the Array List values from Field List in Data Object
        dobject.getFieldValues().clear();
        for (int i = 0; i < dobject.getChildTypes().size(); i++) {
            dobject.getChildren(i).clear();
        }
        //Clear this in the end
        dobject.getChildTypes().clear();
        //System.out.println(" >>>Printing the cleared data object ::\n" + dobject.toString());
    }

    private void deepCleanObjectNode(ObjectNode onode) {
        //onode.removeChildren(); // This is done assuming hierarchy is just one level deep as is the case now.

        if (onode.pGetFieldUpdateLogs() != null) {
            onode.pGetFieldUpdateLogs().clear();
        }
        //Clearing fields
        ObjectField[] ofieldarr = onode.pGetFields();
        for (int i = 0; i < ofieldarr.length; i++) {
            try {
                ObjectField fieldobj = (ObjectField) ofieldarr[i];
                fieldobj.setValue(null);
                fieldobj = null;
            } catch (ObjectException ex) {
                java.util.logging.Logger.getLogger(ObjectFinalizerSlave.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        //Clearing Children
        ArrayList childArr = onode.getAllChildrenFromHashMap();
        if (childArr != null) {
            for (int i = 0; i < childArr.size(); i++) {
                deepCleanObjectNode((ObjectNode) childArr.get(i));
            }
        }

    //System.out.println("   >>>>>>> Printing the cleared data object ::\n" + onode.toString());
    }

    private void goSleep(long time) {
        try {
            sleep(time);
        } catch (InterruptedException ex) {
            sLog.warnNoloc("Problem sleeping with finalizer thread : " + ex.getMessage());
        }
    }

    protected void stopObjectFinalizer() {
        this.finalizerIsRunning = false;
    }

    protected boolean isObjFinThreadRunning() {
        return finalizerIsRunning;
    }
}
