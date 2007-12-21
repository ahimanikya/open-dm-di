/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sun.dm.dimi.datareader;

import com.sun.mdm.index.objects.ObjectField;
import com.sun.mdm.index.objects.ObjectNode;
import com.sun.mdm.index.objects.factory.SimpleFactory;
import com.sun.mdm.index.query.AttributesData;
import com.sun.mdm.index.query.ResultObjectAssembler;
import com.sun.mdm.index.query.VOAException;
import java.util.ArrayList;

/**
 *
 * @author Manish
 */
public class DataObjectNodeAssembler implements ResultObjectAssembler {
    
    DefaultSystemFields systemfields = null;
    
    public DataObjectNodeAssembler() {
        systemfields = new DefaultSystemFields();
    }

    public void init() {
    }

    public Object createRoot(String objectName, AttributesData attrsData) throws VOAException {
        ObjectNode objectNode = null;
        DataObjectNode don = null;
        objectName = stripPath(objectName);

        try {

            objectNode = SimpleFactory.create(objectName);
            don = new DataObjectNode();
            String[] attributeNames = attrsData.getAttributeNames();

            for (int i = 0; i < attributeNames.length; i++) {
                int index = systemfields.isAttributeDefault(attributeNames[i].toUpperCase());

                if (index > -1){
                    //Check if Default System Fields need to be inserted.
                    don.setSystemFieldValue(index, attrsData.get(i).toString());
                }
                else {
                    /*
                    create subnode to store EUID
                     */
                    if (attributeNames[i].equals("EUID")) {
                        ArrayList names = new ArrayList();
                        names.add("EUID");

                        ArrayList values = new ArrayList();
                        values.add("0");

                        ArrayList types = new ArrayList();
                        types.add(new Integer(ObjectField.OBJECTMETA_STRING_TYPE));

                        ObjectNode euidNode = new ObjectNode("EUID", names, types,
                                values);
                        euidNode.setValue(attributeNames[i], attrsData.get(i));

                        objectNode.addChild(euidNode);
                    } else {
                        Object value = attrsData.get(i);
                        int type = objectNode.getField(attributeNames[i]).getType();
                        if (type == ObjectField.OBJECTMETA_INT_TYPE) {
                            // If the return type is BigDecimal, convert to Integer
                            if (value instanceof java.math.BigDecimal) {
                                value = new Integer(((java.math.BigDecimal) value).intValue());
                            }
                        } else if (type == ObjectField.OBJECTMETA_FLOAT_TYPE) {
                            // If the return type is BigDecimal, convert to Float
                            if (value instanceof java.math.BigDecimal) {
                                value = new Float(((java.math.BigDecimal) value).floatValue());
                            }
                        } else if (type == ObjectField.OBJECTMETA_LONG_TYPE) {
                            // If the return type is BigDecimal, convert to Long
                            if (value instanceof java.math.BigDecimal) {
                                value = new Long(((java.math.BigDecimal) value).longValue());
                            }
                        } else if (type == ObjectField.OBJECTMETA_CHAR_TYPE) {
                            // If the return type is Character, convert String value to Character
                            if (value instanceof String) {
                                value = new Character(((String) value).charAt(0));
                            }
                        } else if (type == ObjectField.OBJECTMETA_BOOL_TYPE) {
                            // If the return type is Character, convert String value to Character
                            if (value instanceof java.math.BigDecimal) {
                                value = new Boolean(((java.math.BigDecimal) value).intValue() == 1 ? true : false);
                            }
                        }
                        objectNode.setValue(attributeNames[i], value);
                    }
                }
            }
        } catch (Exception e) {
            throw new VOAException("PLG516: Could not create root:{0} : " + e.getMessage());
        }
        don.setObjectNode(objectNode);
        return don;
    }

    public Object createObjectAttributes(Object rootObject, Object parent, String objectName, AttributesData attrsData) throws VOAException {
        ObjectNode objectNode = null;
        objectName = stripPath(objectName);

        try {
            objectNode = SimpleFactory.create(objectName);

            String[] attributeNames = attrsData.getAttributeNames();

            for (int i = 0; i < attributeNames.length; i++) {
                Object value = attrsData.get(i);
                int type = objectNode.getField(attributeNames[i]).getType();
                if (type == ObjectField.OBJECTMETA_INT_TYPE) {
                    // If the return type is BigDecimal, convert to Integer
                    if (value instanceof java.math.BigDecimal) {
                        value = new Integer(((java.math.BigDecimal) value).intValue());
                    }
                } else if (type == ObjectField.OBJECTMETA_FLOAT_TYPE) {
                    // If the return type is BigDecimal, convert to Float
                    if (value instanceof java.math.BigDecimal) {
                        value = new Float(((java.math.BigDecimal) value).floatValue());
                    }
                } else if (type == ObjectField.OBJECTMETA_LONG_TYPE) {
                    // If the return type is BigDecimal, convert to Long
                    if (value instanceof java.math.BigDecimal) {
                        value = new Long(((java.math.BigDecimal) value).longValue());
                    }
                }
                objectNode.setValue(attributeNames[i], value);
            }

            DataObjectNode don = (DataObjectNode)parent;
            ObjectNode parentNode = (ObjectNode)don.getObjectNode();
            parentNode.addChild(objectNode);
        } catch (Exception e) {
            throw new VOAException("PLG515: Could not create Object attributes :" + e);
        }
        return objectNode;
    }

    private String stripPath(String objectName) {
        int lastIndex = objectName.lastIndexOf('.');
        String objName = objectName.substring(lastIndex + 1);

        return objName;
    }
}
