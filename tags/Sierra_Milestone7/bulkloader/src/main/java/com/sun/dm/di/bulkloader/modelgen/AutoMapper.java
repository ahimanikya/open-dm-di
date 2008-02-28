/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sun.dm.di.bulkloader.modelgen;

import com.sun.dm.di.bulkloader.util.Localizer;
import com.sun.dm.di.bulkloader.util.LogUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import net.java.hulp.i18n.Logger;
import org.netbeans.modules.etl.model.ETLDefinition;
import org.netbeans.modules.sql.framework.model.DBColumn;
import org.netbeans.modules.sql.framework.model.impl.SourceColumnImpl;
import org.netbeans.modules.sql.framework.model.impl.TargetColumnImpl;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * This class takes care mapping source table colums with target table columns.
 * Mapped columns are used to build etl model which gets converted to etl engine file.
 * Note: This is a pure bulk loader utility and does not depend on functionality as implemented 
 * in etl editor code. This chan be changed later.
 * @author Manish Bharani
 */
public class AutoMapper {

    private ETLDefinition etldef = null;
    HashMap<DBColumn, DBColumn> SrcTrgtMap = new HashMap();
    DocumentBuilder builder = null;
    Document etlmodelroot = null;
    //logger
    private static Logger sLog = LogUtil.getLogger(AutoMapper.class.getName());
    private static Localizer sLoc = Localizer.get();

    protected AutoMapper() {
        initdomparser();
    }

    protected AutoMapper(ETLDefinition etldef) {
        this();
        this.etldef = etldef;
    }

    protected void autoMapSourceToTarget() {
        sLog.info(sLoc.x("LDR300: Mapping Source columns to Target DB columns ..."));
        List<DBColumn> dbcolS = etldef.getSQLDefinition().getSourceColumns();
        List<DBColumn> dbcolT = etldef.getSQLDefinition().getTargetColumns();
        // Iterate Source Columns
        Iterator<DBColumn> isrc = dbcolS.iterator();

        while (isrc.hasNext()) {
            DBColumn srccolm = isrc.next();
            // Iterator target Columns
            Iterator<DBColumn> itrgt = dbcolT.iterator();
            while (itrgt.hasNext()) {
                DBColumn trgtcolm = itrgt.next();
                //if ((srccolm.compareTo(trgtcolm)) == 0) {
                if (compareColumns(srccolm, trgtcolm)){
                    SrcTrgtMap.put(srccolm, trgtcolm);
                    break;
                }
            }
        }
    }

    /**
     * This Method compares columns from Source and Target Tables.
     * Columns are compared for 
     * 1. Column Name
     * 2. Column JDBC Type
     * @param srccolm - Source Table column
     * @param trgtcolm - Target Table Column
     * @return
     */
    private boolean compareColumns(DBColumn srccolm, DBColumn trgtcolm) {
        if (srccolm.getName().equalsIgnoreCase(trgtcolm.getName())){
            if (srccolm.getJdbcType() == trgtcolm.getJdbcType()){
                sLog.fine("Source Col : " + srccolm.getName() + " matches with trgt col : " + trgtcolm.getName());
                return true;
            }
        }
        return false;
    }

    protected String insertColumnRefToSrc(String etldef) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(etldef.getBytes());
            etlmodelroot = builder.parse(bais);
            //Look for the Target DB Model to insert Column references
            NodeList dbModelList = etlmodelroot.getElementsByTagName("dbModel");
            for (int i = 0; i < dbModelList.getLength(); i++) {
                Element dbModel = (Element) dbModelList.item(i);

                if (dbModel.getAttribute("type").equals("target")) {
                    // Target DB Model Found, Create a List of dbColumns in the target model
                    NodeList colList = dbModel.getElementsByTagName("dbColumn");
                    for (int j = 0; j < colList.getLength(); j++) {
                        Element trgtColmn = (Element) colList.item(j);
                        //Create a New Element to Add to Column
                        addReferenceElement(trgtColmn);
                    }
                }
                break;
            }
        } catch (SAXException ex) {
            sLog.errorNoloc("[insertColumnRefToSrc]SAXException", ex);
        } catch (IOException ex) {
            sLog.errorNoloc("[insertColumnRefToSrc]IOException", ex);
        }
        // Return the DOM Structute as XML String
        return DomToString();
    }

    private void addReferenceElement(Element trgtColmn) {
        Element newElement = etlmodelroot.createElement("objectRef");
        newElement.setAttribute("refId", getMappedSQLObjIndex(trgtColmn.getAttribute("id")));
        newElement.setAttribute("type", "source_column");
        trgtColmn.appendChild(newElement);
    }

    private String getMappedSQLObjIndex(String targetId) {
        Iterator i = SrcTrgtMap.keySet().iterator();
        while (i.hasNext()) {
            DBColumn srccol = (DBColumn) i.next();
            DBColumn trgtcol = SrcTrgtMap.get(srccol);
            TargetColumnImpl trgtcolimpl = (TargetColumnImpl) trgtcol;
            if (trgtcolimpl.getId().equals(targetId)) {
                return ((SourceColumnImpl) srccol).getId();
            }
        }
        return "";
    }

    private String DomToString() {
        StringWriter sw = new StringWriter();
        try {
            TransformerFactory transfac = TransformerFactory.newInstance();
            Transformer trans = transfac.newTransformer();
            trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
            trans.setOutputProperty(OutputKeys.INDENT, "yes");
            // Print the DOM node
            StreamResult result = new StreamResult(sw);
            DOMSource source = new DOMSource(etlmodelroot);
            trans.transform(source, result);
        } catch (TransformerException ex) {
            sLog.errorNoloc("[DomToString]TransformerException", ex);
        }
        sLog.fine("ETL Model String : " + sw.toString());
        return sw.toString();
    }

    private void initdomparser() {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            builder = factory.newDocumentBuilder();
        } catch (ParserConfigurationException ex) {
            sLog.errorNoloc("[initdomparser]ParserConfigurationException", ex);
        }
    }
}
