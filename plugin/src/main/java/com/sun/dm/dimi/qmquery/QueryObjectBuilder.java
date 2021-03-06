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
 * QueryObjectBuilder.java
 *
 * Created on Aug 10, 2007, 11:30:42 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package com.sun.dm.dimi.qmquery;

import com.sun.dm.dimi.dataobject.metadata.MetaDataManager;
import com.sun.dm.dimi.dataobject.metadata.MetaDataService;
import com.sun.dm.dimi.util.Localizer;
import com.sun.dm.dimi.util.LogUtil;
import com.sun.dm.dimi.util.PluginConstants;
import com.sun.dm.dimi.util.PluginTools;
import com.sun.mdm.index.query.Condition;
import com.sun.mdm.index.query.QueryObject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import net.java.hulp.i18n.LocalizedString;
import net.java.hulp.i18n.Logger;

/**
 * @Title:        CLASS QueryObjectBuilder.java
 * @Description:  Builds the Query Object for Query Manager.
 *                Query Object is used to retrieve data from database by DatabaseDataReader.
 * @Company:      Sun Microsystems
 * @author        Manish Bharani
 */
public class QueryObjectBuilder {

    QueryObject qo = null;
    MetaDataService mdservice = MetaDataManager.getMetaDataManager().getMetaDataService();
    Connection conn = null;
    List<String> selectables = null;
    List<String> conditionsHolder = new ArrayList();
    /**
     * logger
     */
    private static Logger sLog = LogUtil.getLogger(QueryObjectBuilder.class.getName());
    Localizer sLoc = Localizer.get();

    /**
     * Constrictor for QueryObjectBuilder
     */
    public QueryObjectBuilder() {
        sLog.fine("Building Query Object Artifacts for the axion data source");
    }

    /**
     * Constructor for QueryObjectBuilder
     * @param conn
     */
    public QueryObjectBuilder(Connection conn) {
        this();
        this.conn = conn;
    }

    /**
     * createNewQueryObject
     */
    public void createNewQueryObject() {
        qo = new QueryObject();
    }

    /**
     * createNewQueryObject
     * @param prepareid
     */
    public void createNewQueryObject(String prepare_id) {
        if (qo == null) {
            qo = new QueryObject();
            qo.setPreparedId(prepare_id);
        }
    }

    /**
     * Build the Selectables for the Query Object.
     * Selectables are built using Metadata Information contained in the Lookup object.
     * Lookup map is created by parsing objectmap.xml created out of eView designer.
     * It holds the Parent child mapping along with the fields for each hirarchical entity.
     * @param filteredSelects
     */
    public void buildSelectables(ArrayList filteredSelects) {
        if (filteredSelects == null) {
            HashMap lmap = mdservice.getLookupMap();
            Iterator i = lmap.keySet().iterator();
            this.selectables = new ArrayList();
            while (i.hasNext()) {
                String path = (String) i.next();
                addAutoGeneratedIdFields(path);
                HashMap fieldsmap = (HashMap) lmap.get(path);
                Iterator j = fieldsmap.keySet().iterator();
                while (j.hasNext()) {
                    String field = (String) j.next();
                    selectables.add(PluginConstants.QualifiedPathPrefix + path + "." + field); //Constructs fully qualified field names.

                }
            }
            sLog.fine("Selectables :: \n" + PluginTools.printlist(selectables));
        } else {
            selectables = filteredSelects;
            addAutoGeneratedIdFields(mdservice.getHirarchicalRootName());
            Iterator i = mdservice.getChildIndexMap().keySet().iterator();
            while (i.hasNext()) {
                addAutoGeneratedIdFields(i.next().toString());
            }

        }
    }

    /**
     * Query Object Condition Builder
     */
    public void buildConditions() {
        try {
            /*
            Statement stmt = conn.createStatement();
            ResultSet foreignkeyRS = stmt.executeQuery("select" +
            " FKTABLE_NAME, FKCOLUMN_NAME" +
            " from " + PluginConstants.AXION_KEY_TABLE +
            " where FK_NAME IS NOT NULL" +
            " ORDER BY FKTABLE_NAME");
            
            while(foreignkeyRS.next()){
            String normalizedTableName = normalizeDBData(foreignkeyRS.getString(1));
            
            if (checkIfConditionIsValid(normalizedTableName)){
            conditionsHolder.add(PluginConstants.QualifiedPathPrefix +  normalizedTableName + "." + matchFieldCase(normalizedTableName, foreignkeyRS.getString(2)));
            }
            }
             */

            // Add Conditions for the Parent Table as well
            ResultSet pkres = conn.getMetaData().getPrimaryKeys(null, null, PluginConstants.QueryManagerTablePrefix + this.mdservice.getHirarchicalRootName().toUpperCase());
            while (pkres.next()) {
                conditionsHolder.add(PluginConstants.QualifiedPathPrefix + normalizeDBData(pkres.getString(3)) + "." + matchFieldCase(normalizeDBData(pkres.getString(3)), pkres.getString(4)));
            }

        } catch (SQLException ex) {
            sLog.severe(sLoc.x("PLG024: Failed to create SQL Statement \n{0}", ex));
        }
        sLog.fine("Conditions :: \n" + PluginTools.printlist(conditionsHolder));
    }

    private void addAutoGeneratedIdFields(String tablename) {
        addSelectableIfNotAvailable(PluginConstants.QualifiedPathPrefix + tablename + "." + tablename.substring(tablename.lastIndexOf(".") + 1) + "Id");
    }

    private void addSelectableIfNotAvailable(String field) {
        boolean available = false;
        for (int i = 0; i < selectables.size(); i++) {
            if (selectables.get(i).equals(field)) {
                available = true;
                break;
            }
        }
        if (!available) {
            selectables.add(field);
        } else {
            sLog.warn(sLoc.x("PLG023: Field {0 }Already Available. Dropping from selectables list", field));
        }
    }

    /**
     * Sets the seletables for the Query into Query Object
     */
    public void setSeletables() {
        qo.setSelect(selectables.toArray(new String[0]));
    }

    /**
     *
     * @param rootPKColName
     */
    public void checkAndAddRootPKIfNotSelected(String rootPKColName) {
        String normAndQualFieldName = PluginConstants.QualifiedPathPrefix + mdservice.getHirarchicalRootName() + "." + matchFieldCase(mdservice.getHirarchicalRootName(), rootPKColName);
    }

    /**
     * Builds conditions for the query object. Conditions are synonymous with where clause in the SQL statement
     * Conditions are built using the AXION Metadata table for PKs and FKs.
     * FKs for the child tables are equated against PK for the Base (Parent) Table
     * LIMITATIONS - The component assumes that FKs of all the tables point to PK of the root table for creating the join [Enhance later if needed]
     * @param rootPK - Primary Key of the root table in the hirarchy
     */
    public void setQueryConditions(String rootPK) {
        qo.clearQueryConditions(); //Clears the query conditions from the queru object

        for (int i = 0; i < conditionsHolder.size(); i++) {
            qo.addCondition(new Condition(conditionsHolder.get(i), "=", rootPK, true));
        }
    }

    public void setQueryConditions(String[] rootPKs) {
        qo.clearQueryConditions(); //Clears the query conditions from the query object

        for (int i = 0; i < conditionsHolder.size(); i++) {
            //qo.addCondition(new Condition(conditionsHolder.get(i),"IN", rootPKs));
            qo.addCondition(new Condition(conditionsHolder.get(i), rootPKs[0]));
        }
    }

    private boolean checkIfConditionIsValid(String tablename) {
        for (int i = 0; i < selectables.size(); i++) {
            if (selectables.get(i).indexOf(tablename) != -1) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds OrderBY clause for the QueryManager
     * Data Retrieved is ordered according to the primary key of the base table.
     * @param rootTableName - Table Name of the Primary/Root Table
     * @param rootPKColumn - Primary Key Column for the Parent Table
     */
    public void buildOrderBy(String rootTableName, String rootPKColumn) {
        qo.setOrderBy(PluginConstants.QualifiedPathPrefix + rootTableName + "." + rootPKColumn);

    }

    /**
     * This method convers the Table Names stored in the Database Key Metadata table into fully qualified hirarchical names
     * e.g. QueryManager expects tables to be in the format SBYN_ADDRESS. This is converted into fully qualified names like
     * Person.Address
     * Lookup (Metadata) object is used to map table names to hierarchical names.
     */
    private String normalizeDBData(String dbdata) {
        String fullyQualifiedField = null;
        String table_name = dbdata.substring(dbdata.indexOf(PluginConstants.QueryManagerTablePrefix) + PluginConstants.QueryManagerTablePrefix.length(), dbdata.length());

        //Check If table_name is a parent table name
        if (table_name.equalsIgnoreCase(mdservice.getHirarchicalRootName())) {
            // Its a parent table
            fullyQualifiedField = mdservice.getHirarchicalRootName();
        } else {
            // Its a child table, find the qualified name for the same
            Iterator i = this.mdservice.getChildIndexMap().keySet().iterator();
            while (i.hasNext()) {
                String key = (String) i.next();
                if (((key.toLowerCase()).indexOf(table_name.toLowerCase())) != -1) {
                    fullyQualifiedField = key;
                }
            }
        }

        return fullyQualifiedField;
    }

    /**
     * Method to change the case of selected field into case defined with QueryManager.
     * Query Manager is case-sensitive with Fields.
     * e,g. Selecting the field Enterprise.SystemObject.Person.Address.STREETID will throw an error.
     * This needs to be in the form Enterprise.SystemObject.Person.Address.StreetId. (Assuming StreetId is what is defined in ObjectMap.xml)
     */
    private String matchFieldCase(String normTableName, String field) {
        String fieldmatch = null;
        HashMap fieldmap = (HashMap) mdservice.getLookupMap().get(normTableName);
        Iterator i = fieldmap.keySet().iterator();
        while (i.hasNext()) {
            String key = (String) i.next();
            if (key.equalsIgnoreCase(field)) {
                fieldmatch = key;
                break;
            }
        }
        // It may happen that the field is not available in objectmap.xml, look out for this filed in selectables as it may be auto generated there
        if (fieldmatch == null) {
            // Search this in selectables
            for (int j = 0; j < this.selectables.size(); j++) {
                if (this.selectables.get(j).equalsIgnoreCase(PluginConstants.QualifiedPathPrefix + normTableName + "." + field)) {
                    fieldmatch = this.selectables.get(j).substring(this.selectables.get(j).lastIndexOf(".") + 1);
                    ;
                }
            }

            if (fieldmatch == null) {
                sLog.severe(sLoc.x("PLG025: Field [{0}] is not modelled in eview object def file for the table {1}", field, normTableName));
            }
        }
        return fieldmatch;
    }

    /**
     * Prints the SQL created for the Query
     */
    public void printQuerySQLString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append("SELECT  ");

        //Selectables
        for (int i = 0; i < this.selectables.size(); i++) {
            if ((i + 1) == this.selectables.size()) {
                sbuilder.append("\n" + this.selectables.get(i));
            } else {
                sbuilder.append("\n" + this.selectables.get(i) + ",");
            }
        }

        sbuilder.append("\n FROM ");

        // Table Names
        sbuilder.append("\n XYZ - TODO WHERE ");

        //CONDITIONS
        for (int j = 0; j < this.conditionsHolder.size(); j++) {

            if ((j + 1) == this.conditionsHolder.size()) {
                sbuilder.append("\n " + this.conditionsHolder.get(j));
            } else {
                sbuilder.append(this.conditionsHolder.get(j) + " AND ");
            }
        }

        System.out.println("QUERY IS :: " + sbuilder.toString());

    }

    /**
     * Returns instance of Query Object
     * @return QueryObject
     */
    public QueryObject getQueryObject() {
        return this.qo;
    }
}
