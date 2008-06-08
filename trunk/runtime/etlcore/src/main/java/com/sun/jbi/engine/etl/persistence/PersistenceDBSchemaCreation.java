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
 * @(#)$Id: PersistenceDBSchemaCreation.java,v 1.1 2008/03/26 17:34:54 srengara Exp $
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.persistence;

public class PersistenceDBSchemaCreation extends DBSchemaCreation {

   private static String [] PERSISTENCE_TABLES = new String [] { "MessagePipeline" };

    private static PersistenceDBSchemaCreation mSingleton = new PersistenceDBSchemaCreation();
    private static String CREATE_TABLES_SCRIPT = "create_etlse_tables.sql"; //$NON-NLS-1$
    private static String DROP_TABLES_SCRIPT = "drop_etlse_tables.sql"; //$NON-NLS-1$
    private static String TRUNCATE_TABLES_SCRIPT = "truncate_etlse_tables.sql"; //$NON-NLS-1$
    
    public static final String MESSAGEPIPELINE = "MessagePipeLine"; //$NON-NLS-1$
    
    /**
     * Get singleton instance of DBSchemaCreation
     * @return DBSchemaCreation DSchemaCreation
     */
    public static PersistenceDBSchemaCreation getInstance() {
        return mSingleton;
    }
    
   

    @Override
    protected String getCreateScriptName() {
        return CREATE_TABLES_SCRIPT;
    }


    @Override
    protected String getDropScriptName() {
        return DROP_TABLES_SCRIPT;
    }


    @Override
    protected String[] getTabels() {
        return PERSISTENCE_TABLES;
    }


    @Override
    protected String getTruncateScriptName() {
        return TRUNCATE_TABLES_SCRIPT;
    }


}
