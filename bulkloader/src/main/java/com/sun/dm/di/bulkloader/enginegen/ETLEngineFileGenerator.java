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
package com.sun.dm.di.bulkloader.enginegen;

import com.sun.dm.di.bulkloader.modelgen.ETLDefGenerator;
import com.sun.dm.di.bulkloader.util.BLConstants;
import com.sun.dm.di.bulkloader.util.Localizer;
import com.sun.dm.di.bulkloader.util.LogUtil;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish
 */
public class ETLEngineFileGenerator {

    private String gentargetfile = null;
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
        sLog.info(sLoc.x("LDR201: Writing engine file to disk : {0}", enginefile.getAbsolutePath()));

        try {
            fr = new FileWriter(enginefile);
            fr.write(enginefilecontents);
        } catch (IOException ex) {
            sLog.severe(sLoc.x("LDR202: IO Exception while writing engine file : {0}", ex.getMessage()));
        } finally {
            try {
                fr.close();
            } catch (IOException ex) {
                sLog.severe(sLoc.x("LDR203: I/O Exception while writing engine file : {0}", ex.getMessage()));
            }
        }
    }
}
