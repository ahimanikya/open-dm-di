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
package com.sun.dm.di.bulkloader.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish
 */
public class BLTools {

    //logger
    private static Logger sLog = LogUtil.getLogger(BLTools.class.getName());
    private static Localizer sLoc = Localizer.get();

    public BLTools() {
    }

    public static boolean validateDir(String dirpath) {
        if (dirpath != null) {
            File dbdir = new File(dirpath);
            if (dbdir.exists()) {
                if (dbdir.isDirectory()) {
                    return true;
                } else {
                    sLog.severe(sLoc.x("LDR400: Directory is not a valid dir : {0}", dirpath));
                }
            } else {
                sLog.severe(sLoc.x("LDR401: Directory does not exist : {0}", dirpath));
            }
        } else {
            sLog.severe(sLoc.x("LDR402: Path to Package is null"));
            return false;
        }
        return false;
    }

    public static boolean validateFile(String filepath, String filename) {
        if (filename != null) {
            File dbfile = new File(filepath + BLConstants.fs + filename);
            if (dbfile.exists()) {
                if (dbfile.isFile()) {
                    return true;
                } else {
                    sLog.severe(sLoc.x("LDR403: File does not exist : {0}", filename));
                }
            } else {
                sLog.severe(sLoc.x("LDR404: File [ {0} ] does not exist in dir", filepath));
            }
        }
        else{
            sLog.severe(sLoc.x("LDR405: File is Null"));
            return false;
        }
        return false;
    }

    /*
     * Validate if the DB with the name exists
     */
    public static boolean validateAxionDBName(String dbDir, String dbName) {
        boolean ret = false;
        File dbfile = new File(dbDir + BLConstants.fs + dbName.toUpperCase() + BLConstants.AXION_DB_VERSION);
        if (dbfile.exists()) {
            if (dbfile.isFile()) {
                ret = true;
            } else {
                sLog.severe(sLoc.x("LDR406: DataBase file [ {0}.VER ] does not exist in dir : {1}",dbName,dbDir));
            }
        } else {
            sLog.severe(sLoc.x("LDR407: DataBase File does not exist: {0}", dbfile));
        }
        return ret;
    }

    /*
     * Validate if the DB with the name exists
     */
    public static String validateAxionDB(String dbDir) {
        File f = new File(dbDir);
        String[] files = f.list();
        for (int i = 0; i < files.length; i++) {
            if ((files[i].indexOf(".VER")) != -1) {
                return files[i].substring(0, files[i].indexOf(".VER"));
            }
        }
        return null;
    }

    public static boolean validatePath(String filepath, String filename) {
        sLog.fine("Validating File Path [" + filepath + "] for file : " + filename);
        if (validateDir(filepath)) {
            if (validateFile(filepath, filename)) {
                return true;
            }
        }
        return false;
    }

    public static String copySrcDBFileToClassPath(String sourceFileLoc, String filename) {
        File newsourcedir = null;
        try {
            // Create a DB Dir
            String newdirname = null;
            if (filename.indexOf(".") != -1) {
                String containername = filename.substring(0, filename.indexOf(".")).toUpperCase();
                newdirname = containername + BLConstants.fs + BLConstants.EXTDB_PREFIX + containername;
            }

            newsourcedir = new File(BLConstants.toplevelrt + BLConstants.fs + newdirname);
            newsourcedir.mkdirs();
            sLog.fine("Created Dir : " + newsourcedir.getAbsolutePath());

            //Move Source File to This
            copy(new File(sourceFileLoc, filename), new File(newsourcedir.getAbsolutePath(), filename));

        } catch (Exception ex) {
            sLog.errorNoloc("[copySrcDBFileToClassPath] Exception", ex);
        }

        return newsourcedir.getAbsolutePath();
    }

    private static void copy(File source, File dest) throws IOException {
        sLog.fine("Copying file [" + source.getAbsolutePath() + "] to [" + dest.getAbsolutePath() + "]");
        FileChannel in = null, out = null;
        try {
            in = new FileInputStream(source).getChannel();
            out = new FileOutputStream(dest).getChannel();

            long size = in.size();
            MappedByteBuffer buf = in.map(FileChannel.MapMode.READ_ONLY, 0, size);
            out.write(buf);

        } finally {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
        }
    }

    public static void writeIntoFile(String packagename, String filename, String fileContents) {

        FileWriter fr = null;
        File myfile = new File(packagename, filename);
        if (!validateDir(packagename)) {
            new File(packagename).mkdir();
        }

        try {
            fr = new FileWriter(myfile);
            fr.write(fileContents);
        } catch (IOException ex) {
            sLog.errorNoloc("[writeIntoFile] IOException", ex);
        } finally {
            try {
                fr.close();
            } catch (IOException ex) {
                sLog.errorNoloc("[writeIntoFile] IOException", ex);
            }
        }
    }
}
