/*
 * PluginTools.java
 *
 * Created on Sep 25, 2007, 4:46:59 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.dimi.util;

import com.sun.mdm.index.objects.metadata.MetaDataService;
import com.sun.mdm.index.parser.ParserException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;
import net.java.hulp.i18n.LocalizedString;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish Bharani
 */
public class PluginTools {
    
    /**
     * logger
     */
    private static Logger logger = LogUtil.getLogger(PluginTools.class.getName());
    private static Localizer sLoc = Localizer.get();
    
    public PluginTools() {
    }
    
    public static boolean validateDir(String dirpath){
        boolean ret = false;
        File dbdir = new File(dirpath);
        if (dbdir.exists()){
            if (dbdir.isDirectory()){
                ret = true;
            } else{
                logger.severe(sLoc.x("PLG026: Directory {0} is not valid", dirpath));
            }
        } else{
            logger.severe(sLoc.x("PLG027: Directory {0} does not exist", dirpath));
        }
        return ret;
    }
    
    public static boolean validateFile(String filepath, String filename, boolean iseViewModelFile) throws ParserException, FileNotFoundException{
        boolean ret = false;
        File dbfile = new File(filepath + PluginConstants.fs + filename);
        if (dbfile.exists()){
            if (dbfile.isFile()){
                try {
                    if (iseViewModelFile){
                        PluginConstants.setEViewConfigFilePath(dbfile);
                        MetaDataService.registerObjectDefinition(new FileInputStream(dbfile));
                    }
                    ret = true;
                } catch ( ParserException ex) {
                    throw new ParserException("PLG028: Error in Parsing File [" + filepath + PluginConstants.fs + filename  + "]" +  ex.getMessage());
                } catch (FileNotFoundException ex) {
                    throw new FileNotFoundException(sLoc.x("PLG029: File {0} not found in dir {1} \n {2}", filename, filepath, ex).toString());
                }
            } else{
                throw new FileNotFoundException(sLoc.x("PLG030: File {0} not found", filename).toString());
            }
        } else{
            throw new FileNotFoundException(sLoc.x("PLG031: File {0} not found in dir {1}", filename, filepath).toString());
        }
        return ret;
    }
    
    public static boolean validateFile(File configfile) throws ParserException, FileNotFoundException{
        return validateFile(configfile.getParent(), configfile.getName(), false);
    }
    
    public static boolean validateAndSeteViewConfigFile(File configfile)throws ParserException, FileNotFoundException{
        return validateFile(configfile.getParent(), configfile.getName(), true);
    }
    
    public static boolean validatePath(String filepath, String filename) throws ParserException, FileNotFoundException{
        if (validateDir(filepath)){
            if (validateFile(filepath, filename, false)) {
                return true;
            }
        }
        return false;
    }
    
    public static String printlist(List mylist){
        StringBuilder sb = new StringBuilder();
        for (int i=0; i < mylist.size(); i++){
            sb.append(mylist.get(i) + "\n");
        }
        return sb.toString();
    }
    
}
