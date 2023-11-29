package sqlloader;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Utility for converting ResultSets into some Output formats
 */
public class ResultSetParserUtil {

    /**
     * Convert a result set into a JSON Array
     *
     * @param resultSet
     * @return a JSONArray
     * @throws Exception
     *             if something happens
     */
    public static JSONArray convertToJSON(ResultSet resultSet) throws Exception {
        JSONArray jsonArray = new JSONArray();
        if (resultSet != null) {
            while (resultSet.next()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int total_rows = metaData.getColumnCount();
                JSONObject obj = new JSONObject();
                for (int i = 0; i < total_rows; i++) {
                    String rKey = metaData.getColumnLabel(i + 1).toLowerCase();
                    Object rVal = resultSet.getObject(i + 1);
                    obj.put(rKey, rVal);
                }
                jsonArray.put(obj);
            }
        }
        return jsonArray;
    }

    /**
     * Convert a result set into a XML List
     *
     * @param resultSet
     * @return a XML String with list elements
     * @throws Exception
     *             if something happens
     */
    public static String convertToXML(ResultSet resultSet) throws Exception {
        StringBuilder xmlArray = new StringBuilder("<results>\n");
        if (resultSet != null) {
            while (resultSet.next()) {
                xmlArray.append(convertRecordToXML(resultSet, null));            
            }
            xmlArray.append("</results>");
        }
        return xmlArray.toString();
    }


    /**
     * Convert a single result into an XML string
     *
     * @param resultSet
     * @return a XML String with list elements
     * @throws Exception
     *             if something happens
     */
    public static String convertRecordToXML(ResultSet resultSet, String metadata) throws Exception {
        StringBuilder xmlArray = new StringBuilder();
        xmlArray.append("<envelope><headers>");
        xmlArray.append("<IngestDate>" + DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now()) + "</IngestDate>");
        if (metadata != null) {
            for (String metaElement : metadata.split(";")) {
                String[] tuple = metaElement.split(",");
                xmlArray.append("<" + tuple[0] + ">");
                xmlArray.append(tuple[1]);
                xmlArray.append("</" + tuple[0] + ">");
            }
        }
        xmlArray.append("</headers><triples></triples><instance>");

        if (resultSet != null) {
            ResultSetMetaData rstMetaData = resultSet.getMetaData();
            int totalCols = rstMetaData.getColumnCount();
            
            for (int i = 0; i < totalCols; i++) {
                String rKey = rstMetaData.getColumnLabel(i + 1).toLowerCase();
                Object rVal = resultSet.getObject(i + 1);

                //Append start element
                xmlArray.append("<");
                xmlArray.append(rKey.trim());
                xmlArray.append(">");

                //Append value
                xmlArray.append(rVal);

                //Append end element
                xmlArray.append("</");
                xmlArray.append(rKey.trim());
                xmlArray.append(">");
            }
            
            xmlArray.append("</instance></envelope>");
        }
        return xmlArray.toString();
    }
}

