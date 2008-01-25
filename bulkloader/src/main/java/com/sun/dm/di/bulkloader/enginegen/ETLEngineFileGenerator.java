/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sun.dm.di.bulkloader.enginegen;

import com.sun.dm.di.bulkloader.dbconnector.ConnectionFactory;
import com.sun.dm.di.bulkloader.modelgen.ETLDefGenerator;
import com.sun.dm.di.bulkloader.util.BLConstants;
import com.sun.dm.di.bulkloader.util.Localizer;
import com.sun.dm.di.bulkloader.util.LogUtil;
import com.sun.sql.framework.exception.BaseException;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import net.java.hulp.i18n.Logger;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

/**
 *
 * @author Manish
 */
public class ETLEngineFileGenerator {

    private String gentargetfile = null;
    //logger
    private static Logger sLog = LogUtil.getLogger(ETLEngineFileGenerator.class.getName());
    private static Localizer sLoc = Localizer.get();    

    public ETLEngineFileGenerator() {
        this(BLConstants.DEFAULT_ENGINE_NAME);
    }

    public ETLEngineFileGenerator(String engineFileName) {
        sLog.info(sLoc.x("LDR200: Initializing eTL Engine File Generator .."));
        if (engineFileName.indexOf(".") != -1) {
            this.gentargetfile = engineFileName.substring(0, engineFileName.indexOf(".")) + "_engine.xml";
        } else {
            this.gentargetfile = engineFileName + "_engine.xml";
        }
    }

    /*
    public void generateETLEngineFile(File eTLModelFile) throws Exception {
        StringBuffer fileData = new StringBuffer(1000);
        BufferedReader reader = new BufferedReader(new FileReader(eTLModelFile));
        char[] buf = new char[1024];
        int numRead=0;
        while((numRead=reader.read(buf)) != -1){
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
            buf = new char[1024];
        }
        reader.close();
        generateETLEngineFile(fileData.toString());
    }

    public void generateETLEngineFile(String eTLModelFile) throws Exception {
        ByteArrayInputStream bais = null;
        try {
            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
            bais = new ByteArrayInputStream(eTLModelFile.getBytes("UTF-8"));
            Element root = f.newDocumentBuilder().parse(bais).getDocumentElement();
            
            this.etldef.parseXML(root);
        } catch (BaseException ex) {
            Logger.getLogger(ETLDefGenerator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SAXException ex) {
            Logger.getLogger(ETLDefGenerator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ETLDefGenerator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParserConfigurationException ex) {
            Logger.getLogger(ETLDefGenerator.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                bais.close();
            } catch (IOException ex) {
                Logger.getLogger(ETLDefGenerator.class.getName()).log(Level.SEVERE, null, ex);
            }
        }        
    }
    */
    
    public void generateETLEngineFile(ETLDefGenerator etldefgen) {
        EngineFileCodeGen enginecodegen = new EngineFileCodeGen(etldefgen.getETLDefinition());
        String enginefile = enginecodegen.genEnginecode();
        // Write this to the disk
        engineFileWriter(etldefgen.getSourcePackage(), enginefile);
    }

    private void engineFileWriter(String packagename, String enginefilecontents) {
        FileWriter fr = null;
        File enginefile = null;

        String gentarget = BLConstants.artiTop + packagename;
        enginefile = new File(gentarget, gentargetfile);
        sLog.info(sLoc.x("LDR201: Writing engine file to disk : {0}",enginefile.getAbsolutePath()));
        
        try {
            fr = new FileWriter(enginefile);
            fr.write(enginefilecontents);
        } catch (IOException ex) {
            sLog.severe(sLoc.x("LDR202: IO Exception while writing engine file : {0}", ex.getMessage()));
        } finally {
            try {
                fr.close();
            } catch (IOException ex) {
                sLog.severe(sLoc.x("LDR203: IO Exception while writing engine file : {0}", ex.getMessage()));
            }
        }
    }
}
