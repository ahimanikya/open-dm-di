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
 * @(#)EtlMapEntryTable.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl;

import java.util.*;
import javax.xml.namespace.QName;

public class EtlMapEntryTable {
    private List mEntryList;

    
    /** Creates a new instance of DtelMapEntryTable */
    public EtlMapEntryTable() {
        mEntryList = new ArrayList();
    }
    
    public EtlMapEntry findETLEntry(QName operation, QName service) {
        String incomingOperation = operation.getLocalPart().toString().trim();
        String incomingService = service.toString().trim();
        // Search by operation's fullname and service's fullname
        for (Iterator i = mEntryList.iterator(); i.hasNext();) {
            EtlMapEntry etlMapEntry = (EtlMapEntry) i.next();
            String entryOperation = etlMapEntry.getOperation().toString().trim();
            String entryService = etlMapEntry.getService().toString().trim();

            if (incomingService.equalsIgnoreCase(entryService)) {
                return etlMapEntry;
            }
        }
        return null;
    }
    
    public void addEntry(EtlMapEntry entry) {
       if (!mEntryList.contains(entry)) {
           mEntryList.add(entry);
       }
    }
    
    public void removeEntry(EtlMapEntry entry) {
        mEntryList.remove(entry);
    }
    
    public List getEntryListByServiceUnitName(String serviceUnitName) {
        ArrayList list = new ArrayList();
        for (int i = 0, I = mEntryList.size(); i < I; i++) {
            EtlMapEntry entry = (EtlMapEntry)mEntryList.get(i);
            if (entry.getServiceUnitName().equals(serviceUnitName)) {
                list.add(entry);
            }
        }
        return list;
    }
    
    public List getEntryList() {
        return new ArrayList(mEntryList);
    }
}
