/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sun.dm.di.bulkloader.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

/**
 *
 * @author Manish
 */
public class CreateZip {

    ArrayList filenames = null;  //Filenames to Zip
    byte[] buf = new byte[1024]; // Create a buffer for reading the files
    String zipoutdir = null; // Zip Output Dir
    ZipOutputStream zout = null; // Zip Output Stream
    String[] filesToMove = {"lib", BLConstants.toplevelrt};

    public CreateZip() {
        try {
            filenames = new ArrayList();
            String cwd = BLConstants.getCWD();

            zipoutdir = (new File(cwd)).getParent();
            System.out.println("Zip output dir :: " + zipoutdir);
            zout = new ZipOutputStream(new FileOutputStream(zipoutdir + BLConstants.fs + BLConstants.zipname));

        } catch (FileNotFoundException ex) {
            System.out.println("Zip file not found : " + zipoutdir + BLConstants.fs + BLConstants.zipname);
        }
    }

    public void addFile(String abspath) {
        filenames.add(abspath);
    }

    public void createZip() {
        try {
            // Create the ZIP file

            // Compress the files
            for (int i = 0; i < filenames.size(); i++) {
                FileInputStream in = new FileInputStream(filenames.get(i).toString());

                // Add ZIP entry to output stream.
                zout.putNextEntry(new ZipEntry(filenames.get(i).toString()));

                // Transfer bytes from the file to the ZIP file
                int len;
                while ((len = in.read(buf)) > 0) {
                    zout.write(buf, 0, len);
                }

                // Complete the entry
                zout.closeEntry();
                in.close();
            }

            // Complete the ZIP file
            zout.close();
        } catch (IOException e) {
        }
    }

    private void zipDir(String dir2zip) {
        System.out.println("Zipping Dir : " + dir2zip);
        try {
            //create a new File object based on the directory we have to zip File    
            File zipDir = new File(dir2zip);
            //get a listing of the directory content 
            String[] dirList = zipDir.list();
            byte[] readBuffer = new byte[1024];
            int bytesIn = 0;
            //loop through dirList, and zip the files 
            for (int i = 0; i < dirList.length; i++) {
                File f = new File(zipDir, dirList[i]);
                if (f.isDirectory()) {
                    //if the File object is a directory, call this 
                    //function again to add its content recursively 
                    String filePath = f.getPath();
                    zipDir(filePath);
                    //loop again 
                    continue;
                }

                System.out.println("   Include File >> " + dirList[i]);

                //if we reached here, the File object f was not a directory 
                //create a FileInputStream on top of f 

                FileInputStream fis = new FileInputStream(f);
                //create a new zipentryZipEntryZipEntry
                ZipEntry anEntry = new ZipEntry(f.getPath());
                //place the zip entry in the ZipOutputStream object 
                zout.putNextEntry(anEntry);
                //now write the content of the file to the ZipOutputStream 
                while ((bytesIn = fis.read(readBuffer)) != -1) {
                    zout.write(readBuffer, 0, bytesIn);
                }
                //close the Stream 
                fis.close();
                zout.closeEntry();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void closeZipOutStream() {
        try {
            zout.close();
        } catch (ZipException zex) {
            System.out.println(zex.getMessage());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public void createZipPackage(String cwd) {
        // Create a Top Level Package for zip file
        File toppkg = new File(cwd + BLConstants.fs + BLConstants.toplevelpkg);
        toppkg.mkdir();

        //Move relevent files/dir into this package before zipping
        //String[] filesToMove = {"BulkLoader-1.0.jar"};

        for (int i = 0; i < filesToMove.length; i++) {

            File src = new File(cwd + BLConstants.fs + filesToMove[i]);
            File destdir = new File(toppkg, filesToMove[i]);

            try {
                if (src.isDirectory()){
                 copyDirectory(src, destdir);   
                }
                else{
                    copyFile(src, destdir);   
                }
            } catch (IOException ex) {
                System.out.println("Failed to move file to zip package");
            }
        }

        zipDir(toppkg.getName());

        //Close Zip out Stream
        closeZipOutStream();

    }

    private void copyDirectory(File sourceDir, File destDir) throws IOException {

        if (!destDir.exists()) {
            destDir.mkdir();
        }

        File[] children = sourceDir.listFiles();

        for (File sourceChild : children) {
            String name = sourceChild.getName();
            File destChild = new File(destDir, name);
            if (sourceChild.isDirectory()) {
                copyDirectory(sourceChild, destChild);
            } else {
                copyFile(sourceChild, destChild);
            }
        }
    }

    private void copyFile(File source, File dest) throws IOException {

        if (!dest.exists()) {
            dest.createNewFile();
        }
        InputStream in = null;
        OutputStream out = null;
        try {
            in = new FileInputStream(source);
            out = new FileOutputStream(dest);

            // Transfer bytes from in to out
            byte[] wbuffer = new byte[1024];
            int len;
            while ((len = in.read(wbuffer)) > 0) {
                out.write(wbuffer, 0, len);
            }
        } finally {
            in.close();
            out.close();
        }

    }
}
