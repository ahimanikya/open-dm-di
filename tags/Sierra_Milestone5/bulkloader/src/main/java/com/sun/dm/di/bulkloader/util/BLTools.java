/*
 * eTlLoaderTools.java
 *
 * Created on Nov 12, 2007, 10:27:53 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package com.sun.dm.di.bulkloader.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Manish
 */
public class BLTools {

    private static Logger logger = Logger.getLogger(BLTools.class.getName());

    public BLTools() {
    }

    public static boolean validateDir(String dirpath) {
        if (dirpath != null) {
            File dbdir = new File(dirpath);
            if (dbdir.exists()) {
                if (dbdir.isDirectory()) {
                    return true;
                } else {
                    logger.severe("Directory is not a valid dir : " + dirpath);
                }
            } else {
                logger.severe("Directory does not exist : " + dirpath);
            }
        } else {
            System.out.println("Path to Package is null");
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
                    logger.severe("File does not exist : " + filename);
                }
            } else {
                logger.severe("File [ " + filename + " ] does not exist in dir : " + filepath);
            }
        }
        else{
            System.out.println("File is Null");
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
                logger.severe("DataBase file [" + dbName + ".VER] does not exist in dir : " + dbDir);
            }
        } else {
            logger.severe("DataBase File does not exist : " + dbfile);
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
        //System.out.println("Validating File Path [" + filepath + "] for file : " + filename);
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
            //System.out.println("Created Dir : " + newsourcedir.getAbsolutePath());

            //Move Source File to This
            copy(new File(sourceFileLoc, filename), new File(newsourcedir.getAbsolutePath(), filename));


        } catch (Exception ex) {
            Logger.getLogger(BLTools.class.getName()).log(Level.SEVERE, null, ex);
        }

        return newsourcedir.getAbsolutePath();
    }

    private static void copy(File source, File dest) throws IOException {
        //System.out.println("Copying file [" + source.getAbsolutePath() + "] to [" + dest.getAbsolutePath() + "]");
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
            System.out.println("IO Exception");
        } finally {
            try {
                fr.close();
            } catch (IOException ex) {
                System.out.println("IO Exception");
            }
        }
    }
}
