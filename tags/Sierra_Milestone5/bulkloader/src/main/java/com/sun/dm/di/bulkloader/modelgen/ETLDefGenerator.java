/*
 * ETLDefGenerator.java
 *
 * Created on Nov 14, 2007, 1:36:28 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package com.sun.dm.di.bulkloader.modelgen;

import com.sun.etl.engine.ETLEngine;
import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.utils.StringUtil;
import com.sun.dm.di.bulkloader.util.BLConstants;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.netbeans.modules.etl.codegen.DBConnectionDefinitionTemplate;
import org.netbeans.modules.etl.codegen.ETLCodegenUtil;
import org.netbeans.modules.etl.codegen.ETLProcessFlowGenerator;
import org.netbeans.modules.etl.codegen.ETLProcessFlowGeneratorFactory;
import org.netbeans.modules.etl.codegen.impl.InternalDBMetadata;
import org.netbeans.modules.etl.model.ETLDefinition;
import org.netbeans.modules.etl.model.impl.ETLDefinitionImpl;
import org.netbeans.modules.etl.utils.ETLDeploymentConstants;

import org.netbeans.modules.jdbc.builder.DBMetaData;
import org.netbeans.modules.jdbc.builder.ForeignKeyColumn;
import org.netbeans.modules.jdbc.builder.KeyColumn;
import org.netbeans.modules.jdbc.builder.Table;
import org.netbeans.modules.jdbc.builder.TableColumn;
import org.netbeans.modules.model.database.DBConnectionDefinition;


import org.netbeans.modules.sql.framework.common.jdbc.SQLDBConnectionDefinition;
import org.netbeans.modules.sql.framework.model.SQLConstants;
import org.netbeans.modules.sql.framework.model.SQLDBModel;
import org.netbeans.modules.sql.framework.model.SQLDBTable;
import org.netbeans.modules.sql.framework.model.SQLDefinition;
import org.netbeans.modules.sql.framework.model.SQLModelObjectFactory;
import org.netbeans.modules.sql.framework.model.impl.ForeignKeyImpl;
import org.netbeans.modules.sql.framework.model.impl.PrimaryKeyImpl;
import org.netbeans.modules.sql.framework.model.impl.SourceColumnImpl;
import org.netbeans.modules.sql.framework.model.impl.SourceTableImpl;
import org.netbeans.modules.sql.framework.model.impl.TargetColumnImpl;
import org.netbeans.modules.sql.framework.model.impl.TargetTableImpl;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

/**
 *
 * @author Manish
 */
public class ETLDefGenerator {

    //private static ETLDefGenerator etlgenetator = null;
    private ETLDefinition etldef = null;
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

    public ETLDefGenerator() {
    }

    public ETLDefGenerator(String displayName) {
        etldef = new ETLDefinitionImpl(displayName);

    }

    public void addDBModel(Connection conn, String user_table_name, int type, String login, String pw) {
        SQLDBModel model = null;
        try {
            DBMetaData meta = new DBMetaData();
            meta.connectDB(conn);
            DatabaseMetaData dbmeta = conn.getMetaData();
            DBConnectionDefinition def = null;

            switch (type) {
                case BLConstants.SOURCE_TABLE_TYPE:
                    model = SQLModelObjectFactory.getInstance().createDBModel(SQLConstants.SOURCE_DBMODEL);
                    //Assuming Source is always Axion DB
                    String utn = user_table_name.toUpperCase();
                    String s_url = "jdbc:axiondb:" + utn + ":" + "." + BLConstants.fs + BLConstants.EXTDB_PREFIX + utn;
                    model.setModelName(s_url);
                    def = SQLModelObjectFactory.getInstance().createDBConnectionDefinition(meta.getDBName(), meta.getDBType(), BLConstants.DB_AXION_DRIVER, s_url, login, pw, "Bulk Loader Source Model");
                    break;
                case BLConstants.TARGET_TABLE_TYPE:
                    model = SQLModelObjectFactory.getInstance().createDBModel(SQLConstants.TARGET_DBMODEL);
                    // Assuming Target is always Oracle
                    model.setModelName(dbmeta.getURL());
                    def = SQLModelObjectFactory.getInstance().createDBConnectionDefinition(meta.getDBName(), meta.getDBType(), BLConstants.DB_ORACLE_DRIVER, dbmeta.getURL(), login, pw, "Bulk Loader Target Model");
                    break;
            }
            //DBConnectionDefinition def = SQLModelObjectFactory.getInstance().createDBConnectionDefinition(meta.getDBName(), meta.getDBType(), dbmeta.getDriverName(), "MYURL1", "sa", "sa", "eTL Loader Generated Artifact");
            //model.setModelName(dbmeta.getURL());
            //model.setModelName("MYURL2");
            model.setConnectionDefinition(def);

            SQLDBTable ffTable = getTable(meta, "", "", user_table_name, type);
            Table t = getTable(type, meta, ffTable);

            meta.checkForeignKeys(t);
            meta.checkPrimaryKeys(t);

            TableColumn[] cols = t.getColumns();
            TableColumn tc = null;
            List pks = t.getPrimaryKeyColumnList();
            List pkCols = new ArrayList();
            Iterator it = pks.iterator();
            while (it.hasNext()) {
                KeyColumn kc = (KeyColumn) it.next();
                pkCols.add(kc.getColumnName());
            }
            if (pks.size() != 0) {
                PrimaryKeyImpl pkImpl = new PrimaryKeyImpl(((KeyColumn) t.getPrimaryKeyColumnList().get(0)).getName(), pkCols, true);

                switch (type) {
                    case BLConstants.SOURCE_TABLE_TYPE:
                        ((SourceTableImpl) ffTable).setPrimaryKey(pkImpl);
                        break;
                    case BLConstants.TARGET_TABLE_TYPE:
                        ((TargetTableImpl) ffTable).setPrimaryKey(pkImpl);
                        break;
                }

            }
            List fkList = t.getForeignKeyColumnList();
            it = fkList.iterator();
            while (it.hasNext()) {
                ForeignKeyColumn fkCol = (ForeignKeyColumn) it.next();
                ForeignKeyImpl fkImpl = new ForeignKeyImpl((SQLDBTable) ffTable, fkCol.getName(), fkCol.getImportKeyName(),
                        fkCol.getImportTableName(), fkCol.getImportSchemaName(), fkCol.getImportCatalogName(), fkCol.getUpdateRule(),
                        fkCol.getDeleteRule(), fkCol.getDeferrability());
                List fkColumns = new ArrayList();
                fkColumns.add(fkCol.getColumnName());
                String cat = fkCol.getImportCatalogName();
                if (cat == null) {
                    cat = "";
                }
                String sch = fkCol.getImportSchemaName();
                if (sch == null) {
                    sch = "";
                }
                pks = meta.getPrimaryKeys(cat, sch, fkCol.getImportTableName());
                List pkColumns = new ArrayList();
                Iterator pksIt = pks.iterator();
                while (pksIt.hasNext()) {
                    KeyColumn kc = (KeyColumn) pksIt.next();
                    pkColumns.add(kc.getColumnName());
                }
                fkImpl.setColumnNames(fkColumns, pkColumns);

                switch (type) {
                    case BLConstants.SOURCE_TABLE_TYPE:
                        ((SourceTableImpl) ffTable).addForeignKey(fkImpl);
                        break;
                    case BLConstants.TARGET_TABLE_TYPE:
                        ((TargetTableImpl) ffTable).addForeignKey(fkImpl);
                        break;
                }

            }
            for (int j = 0; j < cols.length; j++) {
                tc = cols[j];

                switch (type) {
                    case BLConstants.SOURCE_TABLE_TYPE:
                        SourceColumnImpl ffSourceColumn = new SourceColumnImpl(tc.getName(), tc.getSqlTypeCode(), tc.getNumericScale(), tc.getNumericPrecision(), tc.getIsPrimaryKey(), tc.getIsForeignKey(), false, tc.getIsNullable());
                        ((SourceTableImpl) ffTable).addColumn(ffSourceColumn);
                        break;
                    case BLConstants.TARGET_TABLE_TYPE:
                        TargetColumnImpl ffTargetColumn = new TargetColumnImpl(tc.getName(), tc.getSqlTypeCode(), tc.getNumericScale(), tc.getNumericPrecision(), tc.getIsPrimaryKey(), tc.getIsForeignKey(), false, tc.getIsNullable());
                        ((TargetTableImpl) ffTable).addColumn(ffTargetColumn);
                        break;
                }
            }

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

            // Add Model to ETL Definition
            etldef.addObject(model);

            // Write Default Model to Package
            if (type == BLConstants.SOURCE_TABLE_TYPE) {
                writeModelToPackage();
            }


        } catch (Exception ex) {
            Logger.getLogger("global").log(Level.SEVERE, null, ex);
        }
    }

    public File getETLDefFile() {
        File f = null;
        {
            FileWriter fos = null;
            try {
                f = new File("D:/temp/ETL/manish.etl");
                fos = new FileWriter(f);
                fos.write(etldef.toXMLString(null));
            } catch (IOException ex) {
                Logger.getLogger("global").log(Level.SEVERE, null, ex);
            } catch (BaseException ex) {
                System.out.println("Error Opening ETLDefFile");
            } finally {
                try {
                    fos.close();
                } catch (IOException ex) {
                    Logger.getLogger("global").log(Level.SEVERE, null, ex);
                }
            }
        }
        return f;
    }

    public void printETLDefinition() {
        try {
            System.out.println(etldef.toXMLString(null));
        } catch (BaseException ex) {
            System.out.println("Error printing ETLDefinitionFile");
        }
    }

    public ETLDefinition getETLDefinition() {
        return etldef;
    }

    /*
    public String generateETLEnfineFile() {
    ETLScriptBuilderModel sbmodel = new ETLScriptBuilderModel();
    try {
    sbmodel.setSqlDefinition(etldef.getSQLDefinition());
    //sbmodel.getEngine().createStartETLTaskNode();
    sbmodel.getEngine().createEndETLTaskNode();
    sbmodel.getEngine().createETLTaskNode("INIT");
    //sbmodel.getEngine().getEndETLTaskNode().
    sbmodel.applyConnectionDefinitions();
    } catch (BaseException ex) {
    Logger.getLogger("global").log(Level.SEVERE, null, ex);
    }
    return sbmodel.getEngine().toXMLString();
    }
     */
    public String generateETLEnfineFile(File etlDefFile, File buildDir) {
        ETLEngine engine = null;

        try {
            String etlFileName = etlDefFile.getName().substring(0, etlDefFile.getName().indexOf(".etl"));
            String engineFile = buildDir + "/" + etlFileName + "_engine.xml";

            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
            Element root = f.newDocumentBuilder().parse(etlDefFile).getDocumentElement();

            ETLDefinition def = etldef;
            def.parseXML(root);
            collabName = def.getDisplayName();
            SQLDefinition sqlDefinition = def.getSQLDefinition();

            populateConnectionDefinitions(sqlDefinition);
            sqlDefinition.overrideCatalogNamesForOtd(otdCatalogOverrideMapMap);
            sqlDefinition.overrideSchemaNamesForOtd(otdSchemaOverrideMapMap);

            ETLProcessFlowGenerator flowGen = ETLProcessFlowGeneratorFactory.getCollabFlowGenerator(sqlDefinition, true);
            flowGen.setWorkingFolder(ETLDeploymentConstants.PARAM_APP_DATAROOT);
            flowGen.setInstanceDBName(ETLDeploymentConstants.PARAM_INSTANCE_DB_NAME);
            flowGen.setInstanceDBFolder(ETLCodegenUtil.getEngineInstanceWorkingFolder());
            flowGen.setMonitorDBName(def.getDisplayName());
            flowGen.setMonitorDBFolder(ETLCodegenUtil.getMonitorDBDir(def.getDisplayName(), ETLDeploymentConstants.PARAM_APP_DATAROOT));

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
            Logger.getLogger("global").log(Level.SEVERE, null, ex);
        } catch (ParserConfigurationException ex) {
            Logger.getLogger("global").log(Level.SEVERE, null, ex);
        } catch (SAXException ex) {
            Logger.getLogger("global").log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger("global").log(Level.SEVERE, null, ex);
        }
        System.out.println("Successfully Generated eTL Model File : " + etlDefFile.getName());
        return engine.toXMLString();
    }

    public void writeModelToPackage(String etlModelName) {
        System.out.println("Writing [ " + etlModelName + " ] Model to Package ...");
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
            System.out.println("ETL Def File will be generated into : " + sourcepackage);


            File dumpfile = new File(BLConstants.artiTop + sourcepackage + BLConstants.fs + etl_def_name);
            fwriter = new FileWriter(dumpfile);
            fwriter.write(etldef.toXMLString(null));

        } catch (BaseException ex) {
            Logger.getLogger(ETLDefGenerator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ETLDefGenerator.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                fwriter.close();
            } catch (IOException ex) {
                Logger.getLogger(ETLDefGenerator.class.getName()).log(Level.SEVERE, null, ex);
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

        //System.out.println(connDefs);
        //System.out.println(otdNamePoolNameMap);
        //System.out.println(internalDBConfigParams);

        connDefs.size();
        otdNamePoolNameMap.size();
    }

    /**
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
        connectionParams.put(this.KEY_DATABASE_NAME, collabName);
        connectionParams.put(DBConnectionDefinitionTemplate.KEY_METADATA_DIR, metadataDir);

        conndef.setConnectionURL(StringUtil.replace(conndef.getConnectionURL(), connectionParams));
    }

    private SQLDBTable getTable(DBMetaData dbMeta, String schemaName, String catalogName, String tableName, int type) throws Exception {
        String[][] tableList = dbMeta.getTablesOnly(catalogName, schemaName, "", false);
        SQLDBTable aTable = null;
        String[] currTable = null;
        if (tableList != null) {
            for (int i = 0; i < tableList.length; i++) {
                currTable = tableList[i];
                if (currTable[DBMetaData.NAME].equals(tableName)) {
                    switch (type) {
                        case BLConstants.SOURCE_TABLE_TYPE:
                            aTable = new SourceTableImpl(currTable[DBMetaData.NAME].trim(), currTable[DBMetaData.SCHEMA], currTable[DBMetaData.CATALOG]);
                            break;
                        case BLConstants.TARGET_TABLE_TYPE:
                            aTable = new TargetTableImpl(currTable[DBMetaData.NAME].trim(), currTable[DBMetaData.SCHEMA], currTable[DBMetaData.CATALOG]);
                            break;
                    }
                }
            }
        }
        return aTable;
    }

    private Table getTable(int type, DBMetaData meta, SQLDBTable ffTable) {
        Table t = null;
        try {
            switch (type) {
                case BLConstants.SOURCE_TABLE_TYPE:
                    t = meta.getTableMetaData(((SourceTableImpl) ffTable).getCatalog(), ((SourceTableImpl) ffTable).getSchema(), ((SourceTableImpl) ffTable).getName(), "TABLE");
                    break;
                case BLConstants.TARGET_TABLE_TYPE:
                    t = meta.getTableMetaData(((TargetTableImpl) ffTable).getCatalog(), ((TargetTableImpl) ffTable).getSchema(), ((TargetTableImpl) ffTable).getName(), "TABLE");
                    break;
                default:
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return t;
    }
}