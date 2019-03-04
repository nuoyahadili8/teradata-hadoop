package com.teradata.connector.idatastream.utils;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.idatastream.schema.IDataStreamColumnDesc;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;


/**
 * @author Administrator
 */
public class IDataStreamUtils {
    private static Map<String, Integer> JDBCTypes;
    private static Map<String, Integer> TDTypes;

    public static Map<String, Integer> getJDBCTypesMap() {
        final Map<String, Integer> result = new HashMap<String, Integer>();
        for (final Field field : Types.class.getFields()) {
            try {
                result.put(field.getName(), (Integer) field.get(null));
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e2) {
                e2.printStackTrace();
            }
        }
        return result;
    }

    public static Map<String, Integer> getTDTypesMap() {
        final Map<String, Integer> tdtypes = new HashMap<String, Integer>();
        final Map<String, Integer> jdbctypes = getJDBCTypesMap();
        if (jdbctypes.containsKey("TINYINT")) {
            tdtypes.put("BYTEINT", jdbctypes.get("TINYINT"));
        }
        if (jdbctypes.containsKey("DOUBLE")) {
            tdtypes.put("DOUBLE PRECISION", jdbctypes.get("DOUBLE"));
        }
        if (jdbctypes.containsKey("INTEGER")) {
            tdtypes.put("INTDATE", jdbctypes.get("INTEGER"));
        }
        if (jdbctypes.containsKey("CHAR")) {
            tdtypes.put("ANSIDATE", jdbctypes.get("CHAR"));
        }
        if (jdbctypes.containsKey("BINARY")) {
            tdtypes.put("BYTE", jdbctypes.get("BINARY"));
        }
        if (jdbctypes.containsKey("VARBINARY")) {
            tdtypes.put("VARBYTE", jdbctypes.get("VARBINARY"));
        }
        return tdtypes;
    }

    public static IDataStreamColumnDesc getIDataStreamColumnDescFromString(final String input) throws ConnectorException {
        String typename = "";
        String pands = "";
        Integer JDBCtype = 0;
        Integer precision = 0;
        Integer scale = 0;
        if (input.contains("(")) {
            typename = input.substring(0, input.lastIndexOf("("));
            pands = input.substring(input.lastIndexOf("("));
            if (pands.contains(",")) {
                precision = Integer.parseInt(pands.substring(pands.indexOf("(") + 1, pands.indexOf(",")));
                scale = Integer.parseInt(pands.substring(pands.indexOf(",") + 1, pands.indexOf(")")));
            } else {
                precision = Integer.parseInt(pands.substring(pands.indexOf("(") + 1, pands.indexOf(")")));
            }
        } else {
            typename = input;
        }
        if (IDataStreamUtils.JDBCTypes == null || IDataStreamUtils.TDTypes == null) {
            IDataStreamUtils.JDBCTypes = getJDBCTypesMap();
            IDataStreamUtils.TDTypes = getTDTypesMap();
        }
        if (IDataStreamUtils.JDBCTypes.containsKey(typename)) {
            if (typename.equalsIgnoreCase("float") && IDataStreamUtils.JDBCTypes.containsKey("DOUBLE")) {
                JDBCtype = IDataStreamUtils.JDBCTypes.get("DOUBLE");
            } else {
                JDBCtype = IDataStreamUtils.JDBCTypes.get(typename);
            }
        } else {
            if (!IDataStreamUtils.TDTypes.containsKey(typename)) {
                throw new ConnectorException(60005);
            }
            JDBCtype = IDataStreamUtils.TDTypes.get(typename);
        }
        return new IDataStreamColumnDesc(JDBCtype, precision, scale);
    }
}
