package com.tikalk.xml;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class JsonConverter {

    List<DataField> textFields;

    public JsonConverter() {
        textFields = new ArrayList<DataField>();
    }

    public void addTextField(String field, String value) {
        textFields.add(new DataField(field, value));
    }

     public String asJSON() {

        String json = null;
        try {

            ObjectMapper mapper = new ObjectMapper();
 
            // text part
            ObjectNode textNode = mapper.createObjectNode();
            for (DataField textField : textFields) {
                textNode.put(textField.field, textField.value);
            }
            json = textNode.toString();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return json;
    }
}
