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
package com.sun.dm.di.bulkloader.modelgen;

import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.utils.StringUtil;
import com.sun.dm.di.bulkloader.util.BLConstants;
import com.sun.dm.di.bulkloader.util.Localizer;
import com.sun.dm.di.bulkloader.util.LogUtil;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import net.java.hulp.i18n.Logger;
import org.netbeans.modules.etl.codegen.DBConnectionDefinitionTemplate;
import org.netbeans.modules.etl.codegen.ETLCodegenUtil;
import org.netbeans.modules.etl.codegen.impl.InternalDBMetadata;
import org.netbeans.modules.etl.model.ETLDefinition;
import org.netbeans.modules.etl.model.impl.ETLDefinitionImpl;
import org.netbeans.modules.sql.framework.common.jdbc.SQLDBConnectionDefinition;
import org.netbeans.modules.sql.framework.model.DBColumn;
import org.netbeans.modules.sql.framework.model.DBConnectionDefinition;
import org.netbeans.modules.sql.framework.model.DBMetaDataFactory;
import org.netbeans.modules.sql.framework.model.ForeignKey;
import org.netbeans.modules.sql.framework.model.SQLCondition;
import org.netbeans.modules.sql.framework.model.SQLConstants;
import org.netbeans.modules.sql.framework.model.SQLDBColumn;
import org.netbeans.modules.sql.framework.model.SQLDBModel;
import org.netbeans.modules.sql.framework.model.SQLDBTable;
import org.netbeans.modules.sql.framework.model.SQLDefinition;
import org.netbeans.modules.sql.framework.model.SQLModelObjectFactory;
import org.netbeans.modules.sql.framework.model.SQLObject;
import org.netbeans.modules.sql.framework.model.impl.ForeignKeyImpl;
import org.netbeans.modules.sql.framework.model.impl.PrimaryKeyImpl;
import org.netbeans.modules.sql.framework.model.impl.SourceColumnImpl;
import org.netbeans.modules.sql.framework.model.impl.SourceTableImpl;
import org.netbeans.modules.sql.framework.model.impl.TargetColumnImpl;
import org.netbeans.modules.sql.framework.model.impl.TargetTableImpl;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

/**
 * This class generated eTL definition file from the databse connection/metadata
 * @author Manish
 */
public class ETLDefGenerator {

    private ETLDefinition etldef = null;
    //AutoMapper automapper = null;
    //Engine File Generator
    private HashMap connDefs = new HashMap();
    private Map otdNamePoolNameMap = new HashMap();
    private DBConnectionDefinitionTemplate connectionDefnTemplate;
    private Map internalDBConfigParams = new HashMap();
    private static final String KEY_DATABASE_NAME = "DatabaseName";
    private String collabName;
    Map otdCatalogOverrideMapMap = new HashMap();
    Map otdSchemaOverrideMapMap = new HashMap();
    String sourcefilename = null;
    String packagename = null;
    //logger
    private static Logger sLog = LogUtil.getLogger(ETLDefGenerator.class.getName());
    private static Localizer sLoc = Localizer.get();

    public ETLDefGenerator() {
        etldef = new ETLDefinitionImpl();
    }

    public ETLDefGenerator(String displayName, Integer strategy) {
        sLog.info(sLoc.x("LDR310: Initializing eTL Definition File Generator .."));
        etldef = new ETLDefinitionImpl(displayName);
        etldef.setExecutionStrategyCode(strategy);
        //etldef.getSQLDefinition().setDbInstanceName("TESTDB");
        //etldef.getSQLDefinition().setWorkingFolder("D:/temp/LDR2");
        //this.automapper = new AutoMapper(etldef);
    }

    public void addDBModel(Connection conn, String db, String user_table_name, int type, String login, String pw) {
        SQLDBModel model = null;
        try {
            DBMetaDataFactory meta = new DBMetaDataFactory();
            meta.connectDB(conn);
            DatabaseMetaData dbmeta = conn.getMetaData();
            DBConnectionDefinition def = null;

            //Creating Connection Definitions for eTL definition file
            switch (type) {
                case BLConstants.SOURCE_TABLE_TYPE:
                    model = SQLModelObjectFactory.getInstance().createDBModel(SQLConstants.SOURCE_DBMODEL);
                    //Assuming Source is always Axion DB
                    String utn = user_table_name.toUpperCase();
                    String s_url = "jdbc:axiondb:" + utn + ":" + "." + BLConstants.fs + BLConstants.toplevelrt + BLConstants.fs + utn + BLConstants.fs + BLConstants.EXTDB_PREFIX + utn;
                    model.setModelName(s_url);
                    def = SQLModelObjectFactory.getInstance().createDBConnectionDefinition(meta.getDBName(), meta.getDBType(), BLConstants.DB_AXION_DRIVER, s_url, login, pw, "Bulk Loader Source Model");
                    break;
                case BLConstants.TARGET_TABLE_TYPE:
                    model = SQLModelObjectFactory.getInstance().createDBModel(SQLConstants.TARGET_DBMODEL);
                    model.setModelName(dbmeta.getURL());
                    if (db.equals("ORACLE")) {
                        def = SQLModelObjectFactory.getInstance().createDBConnectionDefinition(meta.getDBName(), meta.getDBType(), BLConstants.DB_ORACLE_DRIVER, dbmeta.getURL(), login, pw, "Bulk Loader Target Model");
                    } else if (db.equals("SQLSERVER")) {
                        def = SQLModelObjectFactory.getInstance().createDBConnectionDefinition(meta.getDBName(), meta.getDBType(), BLConstants.DB_SQLSERVER_DRIVER, dbmeta.getURL(), login, pw, "Bulk Loader Target Model");
                    } else if (db.equals("DERBY")) {
                        def = SQLModelObjectFactory.getInstance().createDBConnectionDefinition(meta.getDBName(), meta.getDBType(), BLConstants.DB_DERBY_DRIVER, dbmeta.getURL(), login, pw, "Bulk Loader Target Model");
                    } else if (db.equals("AXION")) {
                        System.out.println("TDB for Axion Flatfile Targets");
                    }
                    break;
            }
            model.setConnectionDefinition(def);

            SQLDBTable ffTable = getTable(meta, "", "", user_table_name, type);
            PrimaryKeyImpl pks = meta.getPrimaryKeys(ffTable.getCatalog(), ffTable.getSchema(), ffTable.getName());
            Map<String, ForeignKey> fksmap = meta.getForeignKeys(ffTable);
            meta.populateColumns(ffTable);
            List<DBColumn> cols = ffTable.getColumnList();
            DBColumn tc = null;

            // Setting Primary Keys for Tables in eTL definition file
            switch (type) {
                case BLConstants.SOURCE_TABLE_TYPE:
                    ((SourceTableImpl) ffTable).setPrimaryKey(pks);
                    break;
                case BLConstants.TARGET_TABLE_TYPE:
                    ((TargetTableImpl) ffTable).setPrimaryKey(pks);
                    break;
            }

            // Setting Foreign Keys for Tables in eTL definition file
            Iterator it = fksmap.keySet().iterator();
            while (it.hasNext()) {
                ForeignKey fk = (ForeignKey) fksmap.get(it.next());
                ForeignKeyImpl fkImpl = new ForeignKeyImpl(fk);
                fkImpl.setColumnNames(fk.getColumnNames(), fk.getPKColumnNames());

                switch (type) {
                    case BLConstants.SOURCE_TABLE_TYPE:
                        ((SourceTableImpl) ffTable).addForeignKey(fkImpl);
                        break;
                    case BLConstants.TARGET_TABLE_TYPE:
                        ((TargetTableImpl) ffTable).addForeignKey(fkImpl);
                        break;
                }
            }

            // Adding DataBase Table Columns in eTL definition file
            for (int j = 0; j < cols.size(); j++) {
                tc = cols.get(j);
                switch (type) {
                    case BLConstants.SOURCE_TABLE_TYPE:
                        SourceColumnImpl ffSourceColumn = new SourceColumnImpl(tc);
                        ((SourceTableImpl) ffTable).addColumn(ffSourceColumn);
                        break;
                    case BLConstants.TARGET_TABLE_TYPE:
                        TargetColumnImpl ffTargetColumn = new TargetColumnImpl(tc);
                        ((TargetTableImpl) ffTable).addColumn(ffTargetColumn);
                        break;
                }
            }

            // Setting table properties for tables in eTL definition file
            switch (type) {
                case BLConstants.SOURCE_TABLE_TYPE:
                    ((SourceTableImpl) ffTable).setEditable(true);
                    ((SourceTableImpl) ffTable).setSelected(true);
                    ((SourceTableImpl) ffTable).setAliasName("S1");
                    model.addTable((SourceTableImpl) ffTable);
                    break;
                case BLConstants.TARGET_TABLE_TYPE:
                    ((TargetTableImpl) ffTable).setEditable(true);
                    ((TargetTableImpl) ffTable).setSelected(true);
                    ((TargetTableImpl) ffTable).setAliasName("T1");
                    model.addTable((TargetTableImpl) ffTable);
                    break;
            }

            // Add Table Model to ETL Definition
            etldef.addObject(model);

            // Add Table Join Conditions
            // Assuming the target table has been already added to the model
            if (type == BLConstants.SOURCE_TABLE_TYPE) {
                TargetTableImpl tt = (TargetTableImpl) etldef.getTargetTables().get(0);
                SQLCondition cond = tt.getJoinCondition();
                if (cond == null) {
                    SQLModelObjectFactory.getInstance().createSQLCondition(TargetTableImpl.JOIN_CONDITION);
                }

                List<DBColumn> srcColumnList = ((SourceTableImpl) ffTable).getColumnList();
                for (DBColumn srcCol : srcColumnList) {
                    DBColumn col = tt.getColumn(srcCol.getName());
                    if (col != null) {
                        ((TargetColumnImpl) col).setValue((SQLDBColumn) srcCol);
                        if (col.isPrimaryKey()) {
                            cond.addEqualityPredicate((SQLObject) srcCol, (SQLDBColumn) col);
                        }
                    }
                }
                //automapper.autoMapSourceToTarget();
                writeModelToPackage();
            }

        } catch (Exception ex) {
            sLog.errorNoloc("[addDBModel] Exception", ex);
        }
    }

    /**
     * Utility Method to print generated eTL Definition 
     */
    public void printETLDefinition() {
        try {
            sLog.infoNoloc("ETL Def file : \n" + etldef.toXMLString(null));
        } catch (BaseException ex) {
            sLog.errorNoloc("[printETLDefinition] Error printing ETLDefinitionFile", ex);
        }
    }

    /**
     * Get the generated eTL Definition Object
     * @return ETLDefinition
     */
    public ETLDefinition getETLDefinition() {
        return etldef;
    }

    /**
     * eTL Model File Writer
     * @param etlModelName
     */
    public void writeModelToPackage(String etlModelName) {
        sLog.info(sLoc.x("LDR311: Writing [ {0} ] Model to Package ...", etlModelName));
        FileWriter fwriter = null;
        try {
            String etl_def_name = null;

            if (etlModelName.indexOf(".") != -1) {
                etl_def_name = etlModelName.substring(0, etlModelName.indexOf(".") + 1) + "etl";
            } else {
                etl_def_name = etlModelName + ".etl";
            }

            //Source File DB Name
            String sourcepackage = null;
            if ((this.sourcefilename.indexOf(".")) != -1) {
                sourcepackage = this.sourcefilename.substring(0, this.sourcefilename.indexOf(".")).toUpperCase();
            } else {
                sourcepackage = this.sourcefilename.toUpperCase();
            }
            setSourcePackage(sourcepackage);
            sLog.info(sLoc.x("LDR312: ETL Def File will be generated into : {0}", sourcepackage));


            File dumpfile = new File(BLConstants.artiTop + sourcepackage + BLConstants.fs + etl_def_name);
            fwriter = new FileWriter(dumpfile);
            try {
                //String etldefWithRef = automapper.insertColumnRefToSrc(etldef.toXMLString(null));
                //setETLDefinitionFile(etldef.toXMLString(null));
                fwriter.write(etldef.toXMLString(null));
            } catch (BaseException ex) {
                sLog.errorNoloc("[writeModelToPackage] BaseException", ex);
            }
        } catch (IOException ex) {
            sLog.errorNoloc("[writeModelToPackage] IOException", ex);
        } finally {
            try {
                fwriter.close();
            } catch (IOException ex) {
                sLog.errorNoloc("[writeModelToPackage] IOException", ex);
            }
        }
    }

    /**
     * Load Back Modified eTL def file into local variable
     * @param etldefWithRef
     */
    public void setETLDefinitionFile(String etldefWithRef) {
        ByteArrayInputStream bais = null;
        try {
            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
            bais = new ByteArrayInputStream(etldefWithRef.getBytes("UTF-8"));
            Element root = f.newDocumentBuilder().parse(bais).getDocumentElement();
            this.etldef.parseXML(root);
        } catch (BaseException ex) {
            sLog.errorNoloc("[setETLDefinitionFile] BaseException", ex);
        } catch (SAXException ex) {
            sLog.errorNoloc("[setETLDefinitionFile] SAXException", ex);
        } catch (IOException ex) {
            sLog.errorNoloc("[setETLDefinitionFile] IOException", ex);
        } catch (ParserConfigurationException ex) {
            sLog.errorNoloc("[setETLDefinitionFile] ParserConfigurationException", ex);
        } finally {
            try {
                bais.close();
            } catch (IOException ex) {
                sLog.errorNoloc("[setETLDefinitionFile] IOException", ex);
            }
        }
    }

    public void writeModelToPackage() {
        writeModelToPackage(BLConstants.DEFAULT_MODEL_NAME);
    }

    public void setSourceFileDBName(String sourcefilename) {
        this.sourcefilename = sourcefilename;
    }

    public String getSourceFileDBName() {
        return this.sourcefilename;
    }

    public void setSourcePackage(String packagename) {
        this.packagename = packagename;
    }

    public String getSourcePackage() {
        return this.packagename;
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

    /**
     * Initialize table metadata
     * @param iterator
     * @param string
     */
    @SuppressWarnings("unchecked")
    private void initMetaData(Iterator iterator, String dbtable) {

        SQLDBModel element = (SQLDBModel) iterator.next();
        String oid = getSQDBModelOid(element);
        if (oid == null) {// support older version of DBModel
            return;
        }
        SQLDBConnectionDefinition originalConndef = (SQLDBConnectionDefinition) element.getConnectionDefinition();

        if (originalConndef.getDriverClass().equals("org.axiondb.jdbc.AxionDriver")) {
            SQLDBConnectionDefinition conndefTemplate = this.connectionDefnTemplate.getDBConnectionDefinition("STCDBADAPTER");
            SQLDBConnectionDefinition conndef = (SQLDBConnectionDefinition) conndefTemplate.cloneObject();

            setConnectionParams(conndef);

            String key = originalConndef.getName() + "-" + dbtable;
            conndef.setName(key);
            connDefs.put(key, conndef);
            otdNamePoolNameMap.put(oid, key);
            // TODO all the parameters for InternalDBMetadata comes from collab
            // env
            InternalDBMetadata dbMetadata = new InternalDBMetadata("c:\\temp", false, key);
            internalDBConfigParams.put(oid, dbMetadata);
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

    private SQLDBTable getTable(DBMetaDataFactory dbMeta, String schemaName, String catalogName, String tableName, int type) throws Exception {
        tableName = tableName.toUpperCase(); // Needed as source Flatfile case might be not always upper case - CR 6691780
        String[][] tableList = dbMeta.getTablesOnly(catalogName, schemaName, "", false);
        SQLDBTable aTable = null;
        String[] currTable = null;
        if (tableList != null) {
            for (int i = 0; i < tableList.length; i++) {
                currTable = tableList[i];
                if (currTable[DBMetaDataFactory.NAME].equals(tableName)) {
                    switch (type) {
                        case BLConstants.SOURCE_TABLE_TYPE:
                            sLog.fine(sLoc.x("Match Ffound for Source Table : " + tableName));
                            aTable = new SourceTableImpl(currTable[DBMetaDataFactory.NAME].trim(), currTable[DBMetaDataFactory.SCHEMA], currTable[DBMetaDataFactory.CATALOG]);
                            break;
                        case BLConstants.TARGET_TABLE_TYPE:
                            sLog.fine(sLoc.x("Match Ffound for Target Table : " + tableName));
                            aTable = new TargetTableImpl(currTable[DBMetaDataFactory.NAME].trim(), currTable[DBMetaDataFactory.SCHEMA], currTable[DBMetaDataFactory.CATALOG]);
                            break;
                    }
                    break;
                }
            }
        }
        return aTable;
    }
}
