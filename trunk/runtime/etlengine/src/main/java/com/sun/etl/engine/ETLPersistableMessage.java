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
 * @(#)ETLPersistableMessage.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine;

import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.util.Map;

/**
 * Any message that can be persisted in BPEL will need to implement this interface.
 * 
 * @author Jonathan Giron
 * @version 
 */
public interface ETLPersistableMessage {

    /**
     * Setter for Parts
     * 
     * @param theParts for the container
     */
    public void addPart(String partName, Object partValue);

    /**
     * GETs part from name
     * 
     * @param partName
     * @return Object
     */
    public Object getPart(String partName);

    /**
     * getter for Parts
     * 
     * @return theParts for the container
     */
    public Map getParts();

    /**
     * Persists (serialize) this message.
     * 
     * @param dos The DataOutputStream to which the Java Bean message is persisted.
     * @throws Exception upon error.
     */
    public void persist(DataOutputStream dos) throws Exception;

    /**
     * Restore (deserialize) this message.
     * 
     * @param version The version of the message read from persistent store.
     * @param dis The DataInputStream from which the Java Bean message is restored.
     * @throws Exception upon error.
     */
    public void restore(DataInputStream dis) throws Exception;

    /**
     * Setter for Parts
     * 
     * @param theParts for the container
     */
    public void setParts(Map theParts);

}
