/*
 * Main.java
 *
 * Created on Jul 17, 2007, 2:31:59 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.dimi.plugin;

import com.sun.mdm.index.dataobject.InvalidRecordFormat;
import com.sun.dm.dimi.datareader.DataSourceReaderFactory;
import com.sun.dm.dimi.datareader.GlobalDataObjectReader;
import com.sun.dm.dimi.datawriter.DOWriter;
import com.sun.dm.dimi.datawriter.DataObjectWriterFactory;
import com.sun.dm.dimi.qmquery.SelectableFilter;
import com.sun.dm.dimi.util.Localizer;
import com.sun.dm.dimi.util.LogUtil;
import com.sun.mdm.index.dataobject.DataObject;
import com.sun.mdm.index.parser.ParserException;
import java.io.FileNotFoundException;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish
 */
public class Main {
    
    static Logger logger = LogUtil.getLogger(com.sun.dm.dimi.plugin.Main.class.getName());
    static Localizer sLoc = Localizer.get();
    /** Creates a new instance of Main */
    public Main() {
    }
    
    
    private static SelectableFilter createFilter(){
            SelectableFilter sf = new SelectableFilter();
            
            //Person
            sf.addSeletable("PatientView.FirstName");
            //sf.addSeletable("PatientView.gender");
            //sf.addSeletable("PatientView.SSN");
            
            //Address
            //sf.addSeletable("PatientView.AddreSs.PatientViewId");
            sf.addSeletable("PatientView.AddreSs.ciTy");
            //sf.addSeletable("PatientView.AddreSs.zip");
            
            return sf;
    }
    
    public static void main(String[] args) throws InterruptedException, ParserException, FileNotFoundException{
        String howmany = "10K";
        boolean configset = DataSourceReaderFactory.setEViewConfigFilePath("D:/kishor_s1", "object.xml");

        if (configset){
            // DB READER AND WRITER
            GlobalDataObjectReader doReader = DataSourceReaderFactory.getNewDataObjectReader("PERSONPHONETEST", "D:/kishor_s1/PersonPh0124", null, true);
            DOWriter doWriter = DataObjectWriterFactory.getNewDataObjectWriter("D:/kishor_new", "File1_" + howmany  + ".txt", true);
            
            //FILE READER AND WRITER
            //GlobalDataObjectReader doReader = DataSourceReaderFactory.getNewDataObjectReader("C:/Documents and Settings/Manish/Desktop/terry", "good.txt", true, true);
            //DOWriter doWriter = DataObjectWriterFactory.getNewDataObjectWriter("C:/Documents and Settings/Manish/Desktop/terry/manishgen", "File_dump1.txt", true);
            
            System.out.println("Source Data Type is :: " + doReader.getDataSourceType());
            int count = 0;
            
            try {
                while (true){
                    DataObject dobj = doReader.readDataObject();
                    if (dobj == null) break;
                    ++count;
                    if (count%1 == 0){
                        //System.out.println("Fetching Data Object  << << << " + count);
                        //System.out.println("MANISH DO :: \n" + dobj.toString());
                    }
                    doWriter.write(dobj);
                    doReader.submitObjectForFinalization(dobj);
                }
            } catch (InvalidRecordFormat ex) {
                
            }
            doWriter.flush();
        }
    }
    
}
