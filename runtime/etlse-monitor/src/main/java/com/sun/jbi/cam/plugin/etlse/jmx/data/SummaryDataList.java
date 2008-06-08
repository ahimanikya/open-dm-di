package com.sun.jbi.cam.plugin.etlse.jmx.data;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.sun.jbi.cam.plugin.etlse.model.Localizer;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.sun.jbi.cam.plugin.etlse.model.CollabSummary;

/**
 * utility class which hold the list of summary table rows/instance of collab summary  for a given collab
 *
 * @author Sujit Biswas
 *
 */
public class SummaryDataList {
    
    private static transient final Logger mLogger = Logger.getLogger(SummaryDataList.class.getName());

    private static transient final Localizer mLoc = Localizer.get();
    
    private List<CollabSummary> list = new ArrayList<CollabSummary>();
    
    /**
     * @return the list
     */
    public List<CollabSummary> getList() {
        return list;
    }
    
    /**
     * @param list
     *            the list to set
     */
    public void setList(List<CollabSummary> list) {
        this.list = list;
    }
    
        /*
         *
         * <?xml version="1.0" encoding="UTF-8" ?> <select-result>
         * <select-meta-data> <meta-column position = "0" > <attr name = "NAME"
         * value = "EXECUTIONID" /> </meta-column> <meta-column position = "1" >
         * <attr name = "NAME" value = "TARGETTABLE" /> </meta-column> <meta-column
         * position = "2" > <attr name = "NAME" value = "STARTDATE" />
         * </meta-column> <meta-column position = "3" > <attr name = "NAME" value =
         * "ENDDATE" /> </meta-column> <meta-column position = "4" > <attr name =
         * "NAME" value = "EXTRACTED" /> </meta-column> <meta-column position = "5" >
         * <attr name = "NAME" value = "LOADED" /> </meta-column> <meta-column
         * position = "6" > <attr name = "NAME" value = "REJECTED" /> </meta-column>
         * <meta-column position = "7" > <attr name = "NAME" value = "EXCEPTION_MSG" />
         * </meta-column> </select-meta-data> <select-data> <row position = "1" >
         * <data-column position = "0" value = "1" /> <data-column position = "1"
         * value = "T1_CSV_INPUT_TARGET" /> <data-column position = "2" value =
         * "2006-11-24 12:39:46.890" /> <data-column position = "3" value =
         * "2006-11-24 12:39:49.156" /> <data-column position = "4" value = "10" />
         * <data-column position = "5" value = "10" /> <data-column position = "6"
         * value = "0" /> <data-column position = "7" value = "NULL" /> </row> <row
         * position = "2" > <data-column position = "0" value = "2" /> <data-column
         * position = "1" value = "T1_CSV_INPUT_TARGET" /> <data-column position =
         * "2" value = "2006-11-24 13:52:46.765" /> <data-column position = "3"
         * value = "2006-11-24 13:52:50.453" /> <data-column position = "4" value =
         * "10" /> <data-column position = "5" value = "10" /> <data-column position =
         * "6" value = "0" /> <data-column position = "7" value = "NULL" /> </row>
         * </select-data> </select-result>
         *
         */
    
    /**
     * unmarshall string data in a list of summary table rows
     */
    public void unmarshall(String data) {
        Document doc = getDocument(data);
        if(doc != null) {
            NodeList rows = doc.getElementsByTagName("row");
            
            ;
            for (int i = 0; i < rows.getLength(); i++) {
                Node row = rows.item(i);
                
                CollabSummary summary = new CollabSummary();
                summary.unmarshall(row);
                list.add(summary);
                
            }
        }
    }
    
    /**
     * @param data
     * @param doc
     * @return
     */
    private Document getDocument(String data) {
        
        Document doc = null;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            InputSource is = new InputSource(new ByteArrayInputStream(data.getBytes()));
            
            doc = builder.parse(is);
            
        } catch (Exception e) {
            mLogger.log(Level.INFO,mLoc.loc("INFO085: {0}",e.getMessage()));
        }
        return doc;
    }
    
    @Override
    public String toString() {
        
        return list.toString();
    }
    
}
