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
import net.java.hulp.i18n.LocalizedString;
import net.java.hulp.i18n.Logger;

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

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        //System.out.println("Loader Start ...");
        sLog.info(LocalizedString.valueOf(sLoc.t("LDR001: Loader Start ...")));
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

        ConnectionFactory cfact = ConnectionFactory.getConnectionFactory();
        if (BLTools.validateDir(System.getProperty("sourcedb.loc"))) {
            File f = new File(System.getProperty("sourcedb.loc"));
            String[] datafiles = f.list();

            for (int i = 0; i < datafiles.length; i++) {
                ETLDefGenerator etldefgen = new ETLDefGenerator("ETLDEF_" + datafiles[i]);
                DBConnection cc_target = cfact.createTrgtOracleConn(System.getProperty("target.host"), Integer.parseInt(System.getProperty("target.port")), System.getProperty("target.sid"), System.getProperty("target.schema"), System.getProperty("target.catalog"), System.getProperty("target.login"), System.getProperty("target.pw"), datafiles[i], etldefgen);
                DBConnection cc_source = cfact.createSrcConn(System.getProperty("sourcedb.loc"), datafiles[i], System.getProperty("field.delimiter"), System.getProperty("record.delimiter"), cc_target, etldefgen);

                if (cc_source != null) {
                    // Generate ETL Engine File
                    ETLEngineFileGenerator efgen = new ETLEngineFileGenerator();
                    efgen.generateETLEngineFile(etldefgen);
                } else {
                    System.out.println("Source connection is null. Engine file gen aborted");
                }
            // Generate ETL Definition File
                //ETLDefGenerator defgen  = cfact.getETLDefGenerator();
                //defgen.writeModelToPackage();
                //defgen.writeModelToPackage("ManishSource_test.txt");
                //defgen.printETLDefinition();

            }

            //Generate loader triggers and zip
            CreateTriggers gentriggers = new CreateTriggers();
            CreateZip ziputil = new CreateZip();
            gentriggers.createTriggers();
            ziputil.createZipPackage(BLConstants.getCWD());
        } else {
            System.exit(0);
        }


        System.out.println("Loader End.");

    }
}
