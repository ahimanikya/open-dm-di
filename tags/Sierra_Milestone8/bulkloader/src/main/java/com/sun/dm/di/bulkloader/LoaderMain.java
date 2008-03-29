/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sun.dm.di.bulkloader;

import com.sun.dm.di.bulkloader.enginegen.ETLEngineFileGenerator;
import com.sun.dm.di.bulkloader.dbconnector.ConnectionFactory;
import com.sun.dm.di.bulkloader.dbconnector.DBConnection;
import com.sun.dm.di.bulkloader.modelgen.ETLDefGenerator;
import com.sun.dm.di.bulkloader.util.CreateTriggers;
import com.sun.dm.di.bulkloader.util.CreateZip;
import com.sun.dm.di.bulkloader.util.BLConstants;
import com.sun.dm.di.bulkloader.util.BLTools;
import com.sun.dm.di.bulkloader.util.Localizer;
import com.sun.dm.di.bulkloader.util.LogUtil;
import java.io.File;
import java.sql.SQLException;
import java.sql.Statement;
import net.java.hulp.i18n.Logger;
import org.netbeans.modules.etl.codegen.ETLStrategyBuilder;

/**
 *
 * @author Manish
 */
public class LoaderMain {

    //logger
    private static Logger sLog = LogUtil.getLogger(LoaderMain.class.getName());
    private static Localizer sLoc = Localizer.get();

    /** Creates a new instance of Main */
    public LoaderMain() {
    }

    private static String getPWString(String pw) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pw.length(); i++) {
            if ((i % 2) == 0) {
                sb.append("*");
            } else {
                char pwchar = pw.charAt(i);
                sb.append(pwchar);
            }
        }
        return sb.toString();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        sLog.info(sLoc.x("LDR001: Loader Start ..."));
        /*
        // Set the System Variables (Source)
        System.setProperty("sourcedb.loc", "D:\\temp\\mural\\masterindextest");
        System.setProperty("field.delimiter", "|");
        System.setProperty("record.delimiter", "$$$");
        // Set the System Variables (Target)
        System.setProperty("target.host", "localhost");
        System.setProperty("target.port", "1521");
        System.setProperty("target.sid", "orcl");
        System.setProperty("target.catalog", "OE");
        System.setProperty("target.login", "oe");
        System.setProperty("target.pw", "oe");
         */

        //Building up debug logs for parameters passed
        StringBuilder param_debug = new StringBuilder();
        param_debug.append("Parameters passed to the Bulk Loader ::\n");
        param_debug.append("  [1] Data Source\n");
        param_debug.append("\tSource DB Loation : " + System.getProperty("sourcedb.loc") + "\n");
        param_debug.append("\tSource Field Delimiter Type : " + System.getProperty("field.delimiter") + "\n");
        param_debug.append("\tSource Record Delimiter Type : " + System.getProperty("record.delimiter") + "\n");
        param_debug.append("  [2] Data Target\n");
        int target_type_code = Integer.parseInt(System.getProperty("target.type"));
        switch (target_type_code) {
            case 1:
                param_debug.append("\tTarget Database Type : ORACLE (Code : " + target_type_code + ")\n");
                break;
            case 2:
                param_debug.append("\tTarget Database Type : DERBY (Code : " + target_type_code + ")\n");
                break;
            default:
                param_debug.append("\tTarget Database Type : UNKNOWN (Code : " + target_type_code + ")\n");
                break;
        }
        param_debug.append("\tTarget Host name/ip : " + System.getProperty("target.host") + "\n");
        param_debug.append("\tTarget comm port : " + System.getProperty("target.port") + "\n");
        switch (target_type_code) {
            case 1:
                param_debug.append("\tTarget SID : " + System.getProperty("target.id") + "\n");
            case 2:
                param_debug.append("\tTarget DB NAME : " + System.getProperty("target.id") + "\n");
        }
        param_debug.append("\tTarget Schema : " + System.getProperty("target.schema") + "\n");
        param_debug.append("\tTarget Catalog : " + System.getProperty("target.catalog") + "\n");
        param_debug.append("\tTarget Login : " + System.getProperty("target.login") + "\n");
        param_debug.append("\tTarget PW : " + getPWString(System.getProperty("target.pw")) + "\n");
        sLog.info(sLoc.x("LDR002: {0}", param_debug.toString()));


        ConnectionFactory cfact = ConnectionFactory.getConnectionFactory();
        if (BLTools.validateDir(System.getProperty("sourcedb.loc"))) {
            File f = new File(System.getProperty("sourcedb.loc"));
            String[] datafiles = f.list();

            for (int i = 0; i < datafiles.length; i++) {
                ETLDefGenerator etldefgen = new ETLDefGenerator("ETLDEF_" + datafiles[i], ETLStrategyBuilder.EXEC_MODE_SIMPLE);
                DBConnection cc_target = null;
                switch (target_type_code) {
                    case 1:
                        cc_target = cfact.createTrgtOracleConn(System.getProperty("target.host"), Integer.parseInt(System.getProperty("target.port")), System.getProperty("target.id"), System.getProperty("target.schema"), System.getProperty("target.catalog"), System.getProperty("target.login"), System.getProperty("target.pw"), datafiles[i], etldefgen);
                        break;
                    case 2:
                        cc_target = cfact.createTrgtDerbyConn(System.getProperty("target.host"), Integer.parseInt(System.getProperty("target.port")), System.getProperty("target.id"), System.getProperty("target.schema"), System.getProperty("target.catalog"), System.getProperty("target.login"), System.getProperty("target.pw"), datafiles[i], etldefgen);
                        break;
                    default:
                        cc_target = null;
                        break;
                }

                if (BLConstants.getTrgtConnInfo() == null) {
                    BLConstants.setTrgtConnInfo(cc_target.getDBConnectionURI());
                }

                DBConnection cc_source = null;
                if (cc_target != null) {
                    if (cc_target.getDataBaseConnection() != null) {
                        cc_source = cfact.createSrcConn(System.getProperty("sourcedb.loc"), datafiles[i], System.getProperty("field.delimiter"), System.getProperty("record.delimiter"), System.getProperty("target.schema"), System.getProperty("target.catalog"), cc_target, target_type_code, etldefgen);
                    }
                }

                if (cc_source != null) {
                    // Generate ETL Engine File
                    ETLEngineFileGenerator defgen = new ETLEngineFileGenerator();
                    defgen.generateETLEngineFile(etldefgen);
                } else {
                    sLog.severe(sLoc.x("LDR008: Source connection is null. Engine file gen aborted"));
                }

            //Try to disable Target Table Constraints
            //disableTargetTableConstrains(cc_target, datafiles[i]);
            }

            //Generate loader triggers and zip
            CreateTriggers gentriggers = new CreateTriggers();
            CreateZip ziputil = new CreateZip();
            gentriggers.createTriggers();
            ziputil.createZipPackage(BLConstants.getCWD());
        } else {
            sLog.severe(sLoc.x("LDR009: Loader will exit abnormally"));
            System.exit(0);
        }

        sLog.info(sLoc.x("LDR010: Loader Ends."));
    }

    private static void disableTargetTableConstrains(DBConnection cc_target, String filename) {
        try {
            Statement stmt = cc_target.getDataBaseConnection().createStatement();
            //Query All Constraints from the target table being processed
            StringBuilder disablesql = new StringBuilder();
            disablesql.append("ALTER TABLE ");
            if (System.getProperty("target.catalog") != null) {
                disablesql.append(System.getProperty("target.catalog") + "." + getTargetTableQualifiedName(filename));
            } else {
                disablesql.append(getTargetTableQualifiedName(filename));
            }

            disablesql.append(" DISABLE CONSTRAINT ");

            // Name of the constraint on this table
            System.out.println("SQL IS -------------> " + disablesql.toString());

        //stmt.executeUpdate(sql);
        } catch (SQLException ex) {
            sLog.errorNoloc("SQL Exception while trying to disable constraints", ex);
        }
    }

    private static String getTargetTableQualifiedName(String tname) {
        if (tname.indexOf(".") != -1) {
            return tname.substring(0, tname.lastIndexOf("."));
        }
        return tname;
    }
}
