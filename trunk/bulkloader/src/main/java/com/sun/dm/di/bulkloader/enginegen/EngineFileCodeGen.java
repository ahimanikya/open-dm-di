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
package com.sun.dm.di.bulkloader.enginegen;

import com.sun.dm.di.bulkloader.util.Localizer;
import com.sun.dm.di.bulkloader.util.LogUtil;
import com.sun.etl.engine.ETLEngine;
import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.utils.StringUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import net.java.hulp.i18n.Logger;
import org.netbeans.modules.etl.codegen.DBConnectionDefinitionTemplate;
import org.netbeans.modules.etl.codegen.ETLCodegenUtil;
import org.netbeans.modules.etl.codegen.ETLProcessFlowGenerator;
import org.netbeans.modules.etl.codegen.ETLProcessFlowGeneratorFactory;
import org.netbeans.modules.etl.codegen.impl.InternalDBMetadata;
import org.netbeans.modules.etl.model.ETLDefinition;
import org.netbeans.modules.etl.model.impl.ETLDefinitionImpl;
import org.netbeans.modules.sql.framework.common.jdbc.SQLDBConnectionDefinition;
import org.netbeans.modules.sql.framework.model.SQLDBModel;
import org.netbeans.modules.sql.framework.model.SQLDefinition;

/**
 *
 * @author Manish
 */
class EngineFileCodeGen {

    private String etldefstr = null;
    private ETLDefinition etldef = null;
    private HashMap connDefs = new HashMap();
    private Map dbNamePoolNameMap = new HashMap();
    private Map internalDBConfigParams = new HashMap();
    private DBConnectionDefinitionTemplate connectionDefnTemplate;
    private String collabName;
    private static Logger sLog = LogUtil.getLogger(EngineFileCodeGen.class.getName());
    private static Localizer sLoc = Localizer.get();

    protected EngineFileCodeGen() {
    }

    protected EngineFileCodeGen(ETLDefinition etldef) {
        try {
            this.connectionDefnTemplate = new DBConnectionDefinitionTemplate();
            this.etldefstr = etldef.toXMLString(null);
            this.etldef = etldef;
        } catch (BaseException ex) {
            sLog.errorNoloc("Error while converting etl model to xml string (Base Exception)", ex);
        }
    }

    protected EngineFileCodeGen(File etldef) {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(etldef);
            int x = fis.available();
            byte[] b = new byte[x];
            fis.read(b);
            this.etldefstr = new String(b);
        } catch (IOException ex) {
            sLog.errorNoloc("Error while reading etl model file", ex);
        } finally {
            try {
                fis.close();
            } catch (IOException ex) {
                sLog.errorNoloc("Error while reading etl model file", ex);
            }
        }
        this.etldef = new ETLDefinitionImpl();
    }

    protected String genEnginecode() {
        try {
            ETLDefinitionImpl def = (ETLDefinitionImpl) this.etldef;
            collabName = def.getDisplayName();

            SQLDefinition sqlDefinition = def.getSQLDefinition();

            populateConnectionDefinitions(sqlDefinition);
            ETLProcessFlowGenerator flowGen = ETLProcessFlowGeneratorFactory.getCollabFlowGenerator(sqlDefinition, false);
            flowGen.setWorkingFolder(sqlDefinition.getAxiondbWorkingDirectory());
            flowGen.setInstanceDBName("instancedb");
            flowGen.setInstanceDBFolder(ETLCodegenUtil.getEngineInstanceWorkingFolder(sqlDefinition.getAxiondbWorkingDirectory()));
            flowGen.setMonitorDBName(def.getDisplayName());
            flowGen.applyConnectionDefinitions(true, false);
            ETLEngine engine = flowGen.getScript();

            engine.getContext().putValue("AXIONDB_DATA_DIR", sqlDefinition.getAxiondbWorkingDirectory());
            engine.setDisplayName(collabName);

            sqlDefinition.clearOverride(true, true);


            sLog.info(sLoc.x("LDR211: Successfully Generated ETL Engine File from Model : {0}", etldef.getDisplayName()));
            return engine.toXMLString();
        } catch (BaseException ex) {
            java.util.logging.Logger.getLogger(EngineFileCodeGen.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage());
        }
    }
    

    private void populateConnectionDefinitions(SQLDefinition def) {
        List srcDbmodels = def.getSourceDatabaseModels();
        Iterator iterator = srcDbmodels.iterator();
        while (iterator.hasNext()) {
            initMetaData(iterator);
        }

        List trgDbmodels = def.getTargetDatabaseModels();
        iterator = trgDbmodels.iterator();
        while (iterator.hasNext()) {
            initMetaData(iterator);
        }
        connDefs.size();
        dbNamePoolNameMap.size();
    }
    

    /**
     * @param iterator
     * @param string
     */
    @SuppressWarnings(value = "unchecked")
    private void initMetaData(Iterator iterator) {

        SQLDBModel element = (SQLDBModel) iterator.next();
        String oid = getSQDBModelOid(element);
        if (oid == null) {
            // support older version of DBModel
            return;
        }

        SQLDBConnectionDefinition originalConndef = (SQLDBConnectionDefinition) element.getConnectionDefinition();

        if (originalConndef.getDriverClass().equals("org.axiondb.jdbc.AxionDriver")) {
            SQLDBConnectionDefinition conndefTemplate = this.connectionDefnTemplate.getDBConnectionDefinition("STCDBADAPTER");
            SQLDBConnectionDefinition conndef = (SQLDBConnectionDefinition) conndefTemplate.cloneObject();
            setConnectionParams(conndef);

            String key = originalConndef.getName();
            conndef.setName(key);
            connDefs.put(key, conndef);
            dbNamePoolNameMap.put(oid, key);
            // TODO all the parameters for InternalDBMetadata comes from collab
            InternalDBMetadata dbMetadata = new InternalDBMetadata("c:\\temp", false, key);
            internalDBConfigParams.put(oid, dbMetadata);
        } else {
            // jdbc connection
            SQLDBConnectionDefinition conndef = originalConndef;
            String key = originalConndef.getName();
            conndef.setName(key);
            connDefs.put(key, conndef);
            dbNamePoolNameMap.put(oid, key);
        }
    }
    

    /**
     * @param conndef
     */
    @SuppressWarnings(value = "unchecked")
    private void setConnectionParams(SQLDBConnectionDefinition conndef) {
        String metadataDir = ETLCodegenUtil.getEngineInstanceWorkingFolder();
        Map connectionParams = new HashMap();
        connectionParams.put(DBConnectionDefinitionTemplate.KEY_DATABASE_NAME, collabName);
        connectionParams.put(DBConnectionDefinitionTemplate.KEY_METADATA_DIR, metadataDir);
        conndef.setConnectionURL(StringUtil.replace(conndef.getConnectionURL(), connectionParams));
    }
    

    /**
     * @param element
     * @return
     */
    private String getSQDBModelOid(SQLDBModel element) {
        if (element.getAttribute("refKey") == null) {
            return null;
        }
        String oid = (String) element.getAttribute("refKey").getAttributeValue();
        return oid;
    }
}
