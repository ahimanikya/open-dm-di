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
 * @(#)AxionDBQueryImpl.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package org.axiondb.service;

import java.io.File;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.Database;
import org.axiondb.engine.Databases;
import org.axiondb.jdbc.AxionResultSet;
import org.axiondb.parser.AxionSqlParser;
import org.axiondb.service.client.DataColumn;
import org.axiondb.service.client.MetaColumn;
import org.axiondb.service.client.Row;
import org.axiondb.service.client.SelectData;
import org.axiondb.service.client.SelectMetaData;
import org.axiondb.service.client.SelectResult;
import org.axiondb.service.client.XMLWriterVisitor;

/**
 * @author Ritesh Adval
 */
public class AxionDBQueryImpl implements AxionDBQuery {

    public String executeQuery(String dbName, String dbLoc, String query) throws AxionException {
        SelectResult selectResult = executeAxionQuery(dbName, dbLoc, query);
        XMLWriterVisitor visitor = new XMLWriterVisitor();
        visitor.visit(selectResult);
        return visitor.getXml();
    }
    
    public int executeUpdate(String dbName, String dbLoc, String stmt) throws AxionException{
        Database db = Databases.getOrCreateDatabase(dbName, new File(dbLoc));
        
        AxionSqlParser parser = new AxionSqlParser();
        AxionCommand command = parser.parse(stmt);
        if (command != null) {
            return command.executeUpdate(db);
        }
        return -1;
    }

    public SelectResult executeObjectQuery(String dbName, String dbLoc, String query)
            throws AxionException {
        return executeAxionQuery(dbName, dbLoc, query);
    }

    public ResultSet executeResultSetQuery(String dbName, String dbLoc, String query)
            throws AxionException {
        Database db = Databases.getOrCreateDatabase(dbName, new File(dbLoc));
        
        AxionSqlParser parser = new AxionSqlParser();
        AxionCommand command = parser.parse(query);
        if (command != null) {
            AxionResultSet rset = command.executeQuery(db, true);
            return rset;
        }

        return null;
    }

    private SelectResult executeAxionQuery(String dbName, String dbLoc, String query)
            throws AxionException {
        Database db = Databases.getOrCreateDatabase(dbName, new File(dbLoc));
        AxionResultSet rset = null;
        
        try {
            AxionSqlParser parser = new AxionSqlParser();
            AxionCommand command = parser.parse(query);
            SelectResult selectResult = null;
            if (command != null) {
                rset = command.executeQuery(db, true);
                selectResult = createResult(rset);
            }

            return selectResult;
        } catch (SQLException ex) {
            throw new AxionException(ex);
        } finally {
            if (rset != null) {
                try {
                    rset.close();
                } catch (SQLException ignore) {
                    // ignore
                }
            }
        }
    }

    private SelectResult createResult(AxionResultSet rset) throws SQLException {
        SelectResult selectResult = new SelectResult();

        ResultSetMetaData meta = rset.getMetaData();
        SelectMetaData selectMetaData = new SelectMetaData();
        selectResult.setSelectColumns(selectMetaData);

        int count = meta.getColumnCount();
        int[] colWidths = new int[count];
        List[] colValues = new List[count];
        for (int i = 0; i < count; i++) {
            String label = meta.getColumnLabel(i + 1);
            if (label != null) {
                colWidths[i] = label.length();
            }
            colValues[i] = new ArrayList();
            MetaColumn metaColumn = new MetaColumn(i);
            metaColumn.putAttr(MetaColumn.NAME, label);

            selectMetaData.addMetaColumn(metaColumn);
        }

        SelectData selectData = new SelectData();
        selectResult.setSelectData(selectData);

        // find column values and widths
        while (rset.next()) {
            Row row = new Row(rset.getRow());
            selectData.addRow(row);

            for (int i = 0; i < count; i++) {
                String val = rset.getString(i + 1);
                if (rset.wasNull()) {
                    val = "NULL";
                }

                DataColumn dColumn = new DataColumn(val, i);
                row.addColumn(dColumn);
            }
        }

        return selectResult;
    }

    /**
     * @see org.axiondb.service.AxionDBQuery#shutdown(String, String)
     */
    public void shutdown(String dbName, String dbLoc) throws AxionException {
        Database db = Databases.getOrCreateDatabase(dbName, new File(dbLoc));
        if (db != null) {
            db.shutdown();
        }
    }
}
