package com.teradata.connector.teradata.schema;

import com.teradata.connector.common.exception.ConnectorException;

import java.util.ArrayList;
import java.util.List;


public class TeradataViewDesc {
    private String name;
    private String databaseName;
    private String query;
    private List<TeradataColumnDesc> columns;
    private Boolean accessLock;

    public TeradataViewDesc() {
        this.name = "";
        this.databaseName = "";
        this.query = "";
        this.columns = new ArrayList<TeradataColumnDesc>();
        this.accessLock = false;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
    }

    public void setQuery(final String query) throws ConnectorException {
        if (!query.toUpperCase().trim().startsWith("SEL") && !query.toUpperCase().trim().startsWith("SELECT")) {
            throw new IllegalArgumentException("Only SELECT query is supported");
        }
        this.query = query;
    }

    public void setColumns(final TeradataColumnDesc[] columns) {
        this.columns.clear();
        if (columns != null) {
            for (int i = 0; i < columns.length; ++i) {
                this.columns.add(columns[i]);
            }
        }
    }

    public void addColumnInfo(final TeradataColumnDesc columnInfo) {
        if (columnInfo != null) {
            this.columns.add(columnInfo);
        }
    }

    public void setAccessLock(final Boolean accessLock) {
        this.accessLock = accessLock;
    }

    public String getName() {
        return this.name;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public String getQualifiedName() {
        String result = "";
        if (!this.databaseName.isEmpty()) {
            result = result + this.quoteName(this.databaseName) + ".";
        }
        result += this.quoteName(this.name);
        return result;
    }

    public String getQuery() {
        return this.query;
    }

    public TeradataColumnDesc[] getColumns() {
        return this.columns.toArray(new TeradataColumnDesc[0]);
    }

    public TeradataColumnDesc getColumn(final int index) {
        if (index < 0 || index >= this.columns.size()) {
            return null;
        }
        return this.columns.get(index);
    }

    public String getColumnsString() {
        final StringBuilder sb = new StringBuilder();
        for (int columnCount = this.columns.size(), i = 0; i < columnCount; ++i) {
            sb.append(this.quoteName(this.columns.get(i).getName())).append(",");
        }
        if (sb.length() > 1) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    public Boolean getAccessLock() {
        return this.accessLock;
    }

    private String quoteName(final String objectName) {
        if (objectName == null || objectName.isEmpty()) {
            return "";
        }
        if (objectName.charAt(0) == '\"' && objectName.charAt(objectName.length() - 1) == '\"') {
            return objectName;
        }
        return '\"' + objectName + '\"';
    }
}
