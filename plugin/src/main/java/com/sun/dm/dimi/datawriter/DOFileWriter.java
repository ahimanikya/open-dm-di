/*
 * DOFileWriter.java
 *
 * Created on Sep 4, 2007, 1:22:44 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.dimi.datawriter;

import com.sun.dm.dimi.util.Localizer;
import com.sun.dm.dimi.util.LogUtil;
import com.sun.dm.dimi.util.PluginConstants;
import com.sun.mdm.index.dataobject.DataObject;
import com.sun.mdm.index.dataobject.DataObjectFileWriter;
import java.io.File;
import java.io.IOException;
import net.java.hulp.i18n.LocalizedString;
import net.java.hulp.i18n.Logger;


/**
 * @Title:        CLASS DOFileWriter.java
 * @Description:  This class is used for writing Data Objects into Flat Files (Good/Reject Files)
 * @Company:      Sun Microsystems
 * @author        Manish Bharani
 */
public class DOFileWriter extends DOBaseWriter implements DOWriter {
    
    private File outputFile = null;
    private Boolean isGoodFile = false;
    //private BufferedDataOutputStream bdOutputStream = null;
    //private BufferedWriter bdOutputStream = null;
    private DataObjectFileWriter doeViewFileWriter = null;
    private int flushcount = 0;
    
    /**
     * logger
     */
    private static Logger sLog = LogUtil.getLogger(DOFileWriter.class.getName());
    Localizer sLoc = Localizer.get();
    
    /**
     * Constructor for DOFileWriter
     */
    public DOFileWriter() {
    }
    
    /**
     * Constructor for DOFileWriter
     * @param outputFile
     * @param isGoodFile
     * @param specialMode
     */
    public DOFileWriter(File outputFile, boolean isGoodFile, boolean specialMode){
        super(specialMode);
        try {
            this.outputFile = outputFile;
            this.isGoodFile = isGoodFile;
            this.doeViewFileWriter = new DataObjectFileWriter(outputFile.getAbsolutePath(), specialMode);
            //initReader();
        }
        catch (IOException ex) {
            sLog.severe(sLoc.x("PLG021: Unable to write to file {0} \n{1}", outputFile.getAbsolutePath(),ex));
        }
    }
    
    
    /**
     * Method to write DataObjects into Flat File
     * @param dataobj 
     */
    public void write(DataObject dataobj) {
        try {
            doeViewFileWriter.writeDataObject(dataobj);
            flushcount++;
            if (flushcount > PluginConstants.flushfreq) flush();
            //String datarecord = this.doeViewFileWriter.getDataObjectString(dataobj);
            //System.out.println("Data Record\n" + datarecord + "\n");
            //this.bdOutputStream.writeChars(datarecord);
            //this.bdOutputStream.write(datarecord);
            //this.bdOutputStream.newLine();
            //this.bdOutputStream.flush();
        } catch (IOException ex) {
            sLog.severe(sLoc.x("PLG022: Error Writing Data Object \n{0}", ex));
        }
    }
    
    
    /**
     * Performs initialization for the Axion File System readers
     */
    /*
    private void initReader(){
        try {
            AxionFileSystem axionFileSys = new AxionFileSystem();
            this.bdOutputStream = axionFileSys.createBufferedDOS(this.outputFile);
        } catch (AxionException ex) {
            ex.printStackTrace();
        }
    }
    */
    /*
    private void initReader(){
        try {
            this.bdOutputStream = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.outputFile)));
        }
        catch (FileNotFoundException ex) {
            Logger.getLogger("global").log(Level.SEVERE, null, ex);
        }
    }
    */
    
    public void flush() {
        try {
            doeViewFileWriter.flush();
            sLog.fine("Auto Flushing Data Object Writer Buffer");
            flushcount = 0;
        }
        catch (IOException ex) {
            sLog.fine("IOException", ex);
        }
    }
    
    
}
