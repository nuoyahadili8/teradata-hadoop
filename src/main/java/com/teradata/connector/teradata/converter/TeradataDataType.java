package com.teradata.connector.teradata.converter;

import com.teradata.connector.common.converter.ConnectorDataTypeDefinition;

import java.sql.SQLException;
import java.sql.SQLXML;


public interface TeradataDataType {

    public enum TeradataDataTypeImpl implements TeradataDataType {
        SQLXML(1) {
            @Override
            public int getType() {
                return TeradataDataTypeDefinition.TYPE_SQLXML;
            }

            @Override
            public Object transform(Object object) {
                if (object == null) {
                    return null;
                }
                try {
                    return ((SQLXML) object).getString().replace("&quot;", "\"").replace("&lt;", "<").replace("&gt;", ">");
                } catch (SQLException e) {
                    e.printStackTrace();
                    return object;
                }
            }
        },
        OTHER(99) {
            @Override
            public final int getType() {
                return ConnectorDataTypeDefinition.TYPE_OTHER;
            }

            @Override
            public Object transform(Object object) {
                return object == null ? null : object;
            }
        };

        private int index;

        private TeradataDataTypeImpl(int index) {
            this.index = 0;
            this.index = index;
        }

        public int getIndex() {
            return this.index;
        }
    }

    int getType();

    Object transform(Object obj);
}