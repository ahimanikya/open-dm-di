/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sun.dm.di.bulkloader.enginegen;

import com.sun.dm.di.bulkloader.dbconnector.ConnectionFactory;
import com.sun.dm.di.bulkloader.modelgen.ETLDefGenerator;
import com.sun.dm.di.bulkloader.util.Localizer;
import com.sun.dm.di.bulkloader.util.LogUtil;
import com.sun.etl.engine.ETLEngine;
import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.utils.StringUtil;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import net.java.hulp.i18n.Logger;
import org.netbeans.modules.etl.codegen.DBConnectionDefinitionTemplate;
import org.netbeans.modules.etl.codegen.ETLCodegenUtil;
import org.netbeans.modules.etl.codegen.ETLProcessFlowGenerator;
import org.netbeans.modules.etl.codegen.ETLProcessFlowGeneratorFactory;
import org.netbeans.modules.etl.model.ETLDefinition;
import org.netbeans.modules.etl.model.impl.ETLDefinitionImpl;
import org.netbeans.modules.etl.utils.ETLDeploymentConstants;
import org.netbeans.modules.sql.framework.common.jdbc.SQLDBConnectionDefinition;
import org.netbeans.modules.sql.framework.model.SQLDBModel;
import org.netbeans.modules.sql.framework.model.SQLDefinition;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

/**
 *
 * @author Manish
 */
class EngineFileCodeGen {

    private String etldefstr = null;
    private ETLDefinition etldef = null;
    private HashMap connDefs = new HashMap();
    private Map otdNamePoolNameMap = new HashMap();
    private Map internalDBConfigParams = new HashMap();
    private static final String KEY_DATABASE_NAME = "DatabaseName";
    private String collabName;
    Map otdCatalogOverrideMapMap = new HashMap();
    Map otdSchemaOverrideMapMap = new HashMap();
    //logger
    private static Logger sLog = LogUtil.getLogger(EngineFileCodeGen.class.getName());
    private static Localizer sLoc = Localizer.get();    

    protected EngineFileCodeGen() {

    }

    protected EngineFileCodeGen(ETLDefinition etldef) {
        try {
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
        ETLEngine engine = null;

        try {
            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
            ByteArrayInputStream bais = new ByteArrayInputStream(this.etldefstr.getBytes("UTF-8"));
            Element root = f.newDocumentBuilder().parse(bais).getDocumentElement();

            etldef.parseXML(root);
            collabName = etldef.getDisplayName();
            SQLDefinition sqlDefinition = etldef.getSQLDefinition();

            populateConnectionDefinitions(sqlDefinition);
            sqlDefinition.overrideCatalogNamesForDb(otdCatalogOverrideMapMap);
            sqlDefinition.overrideSchemaNamesForDb(otdSchemaOverrideMapMap);

            ETLProcessFlowGenerator flowGen = ETLProcessFlowGeneratorFactory.getCollabFlowGenerator(sqlDefinition, true);
            flowGen.setWorkingFolder(ETLDeploymentConstants.PARAM_APP_DATAROOT);
            flowGen.setInstanceDBName(ETLDeploymentConstants.PARAM_INSTANCE_DB_NAME);
            flowGen.setInstanceDBFolder(ETLCodegenUtil.getEngineInstanceWorkingFolder());
            flowGen.setMonitorDBName(etldef.getDisplayName());
            flowGen.setMonitorDBFolder(ETLCodegenUtil.getMonitorDBDir(etldef.getDisplayName(), ETLDeploymentConstants.PARAM_APP_DATAROOT));

            if (connDefs.isEmpty()) {
                // TODO change the logic to read connDefs from env, now keep it same
                // a design time
                flowGen.applyConnectionDefinitions(false);
            } else {
                flowGen.applyConnectionDefinitions(connDefs, this.otdNamePoolNameMap,
                        internalDBConfigParams);
            }

            engine = flowGen.getScript();
            sqlDefinition.clearOverride(true, true);

        } catch (BaseException ex) {
            sLog.errorNoloc("Base Exception", ex);
        } catch (ParserConfigurationException ex) {
            sLog.errorNoloc("ParserConfigurationException", ex);
        } catch (SAXException ex) {
            sLog.errorNoloc("SAXException", ex);
        } catch (IOException ex) {
            sLog.errorNoloc("IOException", ex);
        }
        sLog.info(sLoc.x("LDR211: Successfully Generated eTL Engine File from Model : {0}", etldef.getDisplayName()));
        return engine.toXMLString();
    }

    private void populateConnectionDefinitions(SQLDefinition def) {
        List srcDbmodels = def.getSourceDatabaseModels();
        Iterator iterator = srcDbmodels.iterator();
        while (iterator.hasNext()) {
            initMetaData(iterator, "source");
        }

        List trgDbmodels = def.getTargetDatabaseModels();
        iterator = trgDbmodels.iterator();
        while (iterator.hasNext()) {
            initMetaData(iterator, "target");
        }

        connDefs.size();
        otdNamePoolNameMap.size();
    }

    @SuppressWarnings("unchecked")
    private void initMetaData(Iterator iterator, String dbtable) {

        SQLDBModel element = (SQLDBModel) iterator.next();
        String oid = getSQDBModelOid(element);
        if (oid == null) {// support older version of DBModel
            return;
        }
        SQLDBConnectionDefinition originalConndef = (SQLDBConnectionDefinition) element.getConnectionDefinition();

        if (originalConndef.getDriverClass().equals("org.axiondb.jdbc.AxionDriver")) {
            SQLDBConnectionDefinition conndef = originalConndef;
            setConnectionParams(conndef);

            String key = originalConndef.getName() + "-" + dbtable;
            conndef.setName(key);
            connDefs.put(key, conndef);
            otdNamePoolNameMap.put(oid, key);
        // TODO all the parameters for InternalDBMetadata comes from collab
            // env
            //InternalDBMetadata dbMetadata = new InternalDBMetadata("c:\\temp", false, key);
            //internalDBConfigParams.put(oid, dbMetadata);
        } else { // jdbc connection

            SQLDBConnectionDefinition conndef = originalConndef;


            String key = originalConndef.getName() + "-" + dbtable;
            conndef.setName(key);
            connDefs.put(key, conndef);
            otdNamePoolNameMap.put(oid, key);
        // TODO all the parameters for InternalDBMetadata comes from collab
            // env
            //InternalDBMetadata dbMetadata = new InternalDBMetadata("c:\\temp", false, key);
            //internalDBConfigParams.put(oid, dbMetadata);
        }

    }

    private String getSQDBModelOid(SQLDBModel element) {
        if (element.getAttribute("refKey") == null) {
            return null;
        }
        String oid = (String) element.getAttribute("refKey").getAttributeValue();
        return oid;
    }

    @SuppressWarnings("unchecked")
    private void setConnectionParams(SQLDBConnectionDefinition conndef) {
        String metadataDir = ETLCodegenUtil.getEngineInstanceWorkingFolder();
        Map connectionParams = new HashMap();
        connectionParams.put(KEY_DATABASE_NAME, collabName);
        connectionParams.put(DBConnectionDefinitionTemplate.KEY_METADATA_DIR, metadataDir);

        conndef.setConnectionURL(StringUtil.replace(conndef.getConnectionURL(), connectionParams));
    }
}
