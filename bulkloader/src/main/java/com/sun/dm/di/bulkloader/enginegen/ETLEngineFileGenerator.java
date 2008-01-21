/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sun.dm.di.bulkloader.enginegen;

import com.sun.dm.di.bulkloader.modelgen.ETLDefGenerator;
import com.sun.dm.di.bulkloader.util.BLConstants;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;

/**
 *
 * @author Manish
 */
public class ETLEngineFileGenerator {

    private String gentargetfile = null;

    public ETLEngineFileGenerator() {
        this(BLConstants.DEFAULT_ENGINE_NAME);
    }

    public ETLEngineFileGenerator(String engineFileName) {
        System.out.println("Init eTLEnfineFileGenerator ..");
        if (engineFileName.indexOf(".") != -1) {
            this.gentargetfile = engineFileName.substring(0, engineFileName.indexOf(".")) + "_engine.xml";
        } else {
            this.gentargetfile = engineFileName + "_engine.xml";
        }
    }

    public void generateETLEngineFile(File eTLModelFile) throws Exception {
        throw new Exception("Unsupported Method.");
    }

    public void generateETLEngineFile(String eTLModelFile) throws Exception {
        File f = new File(eTLModelFile);
        generateETLEngineFile(f);
    }

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

        try {
            fr = new FileWriter(enginefile);
            fr.write(enginefilecontents);
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
