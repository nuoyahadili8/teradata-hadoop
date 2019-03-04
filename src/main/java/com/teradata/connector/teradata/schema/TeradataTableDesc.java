package com.teradata.connector.teradata.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TeradataTableDesc {
    private String name;
    private String databaseName;
    private boolean multiset;
    private boolean hasPrimaryIndex;
    private boolean hasPartitionColumns;
    private int blockSize;
    private ArrayList<TeradataColumnDesc> columns;
    private List<String> primaryIndices;
    private List<String> partitionColumnNames;

    public TeradataTableDesc() {
        this.name = "";
        this.databaseName = "";
        this.multiset = true;
        this.hasPrimaryIndex = true;
        this.hasPartitionColumns = true;
        this.blockSize = 0;
        this.columns = null;
        this.primaryIndices = null;
        this.partitionColumnNames = null;
        this.columns = new ArrayList<TeradataColumnDesc>();
        this.primaryIndices = new ArrayList<String>();
        this.partitionColumnNames = new ArrayList<String>();
    }

    public void setBlockSize(final int blockSize_) {
        this.blockSize = blockSize_;
    }

    public int getBlockSize() {
        return this.blockSize;
    }

    public void setName(final String name_) {
        this.name = name_;
    }

    public void setDatabaseName(final String databaseName_) {
        this.databaseName = databaseName_;
    }

    public void setMultiset(final boolean multiset_) {
        this.multiset = multiset_;
    }

    public void setColumns(final TeradataColumnDesc[] columns_) {
        this.columns.clear();
        if (columns_ != null) {
            for (int i = 0; i < columns_.length; ++i) {
                this.columns.add(columns_[i]);
            }
        }
    }

    public void addColumn(final TeradataColumnDesc columnInfo_) {
        if (columnInfo_ != null) {
            this.columns.add(columnInfo_);
        }
    }

    public void setHasPrimaryIndex(final boolean hasPrimaryIndex_) {
        this.hasPrimaryIndex = hasPrimaryIndex_;
    }

    public void addPrimaryIndex(final String primaryIndexName_) {
        if (primaryIndexName_ != null && !primaryIndexName_.isEmpty()) {
            this.primaryIndices.add(primaryIndexName_);
            this.hasPrimaryIndex = true;
        }
    }

    public void removePrimaryIndex(final int index_) {
        if (index_ >= 0 && this.primaryIndices.size() > index_) {
            this.primaryIndices.remove(index_);
        }
    }

    public void setHasPartitionColumns(final boolean hasPartitionColumns_) {
        this.hasPartitionColumns = hasPartitionColumns_;
    }

    public void addPartitionColumn(final String partitionColumn_) {
        if (partitionColumn_ != null && !partitionColumn_.isEmpty()) {
            this.partitionColumnNames.add(partitionColumn_);
        }
    }

    public void removePartitionColumn(final int index_) {
        if (index_ >= 0 && this.partitionColumnNames.size() > index_) {
            this.partitionColumnNames.remove(index_);
        }
    }

    public String getName() {
        return this.name;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public String getQualifiedName() {
        String result = "";
        if (this.databaseName != null && !this.databaseName.isEmpty()) {
            result = result + this.quoteName(this.databaseName) + ".";
        }
        result += this.quoteName(this.name);
        return result;
    }

    public boolean isMultiset() {
        return this.multiset;
    }

    public TeradataColumnDesc[] getColumns() {
        return this.columns.toArray(new TeradataColumnDesc[0]);
    }

    public String[] getColumnNames() {
        final String[] columnNames = new String[this.columns.size()];
        for (int i = 0; i < this.columns.size(); ++i) {
            columnNames[i] = this.columns.get(i).getName();
        }
        return columnNames;
    }

    public Map<String, Integer> getColumnNameMap() {
        final Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < this.columns.size(); ++i) {
            map.put(this.columns.get(i).getName(), i);
        }
        return map;
    }

    public Map<String, Integer> getColumnNameLowerCaseMap() {
        final Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < this.columns.size(); ++i) {
            map.put(this.columns.get(i).getName().toLowerCase(), i);
        }
        return map;
    }

    public TeradataColumnDesc getColumn(final int index_) {
        if (index_ < 0 || index_ >= this.columns.size()) {
            return null;
        }
        return this.columns.get(index_);
    }

    public boolean hasPrimaryIndex() {
        return this.hasPrimaryIndex;
    }

    public List<String> getPrimaryIndices() {
        return this.primaryIndices;
    }

    public boolean hasPartitionColumns() {
        return this.hasPartitionColumns;
    }

    public List<String> getPartitionColumnNames() {
        return this.partitionColumnNames;
    }

    public String getColumnsString() {
        final StringBuilder sb = new StringBuilder();
        for (int columnCount = this.columns.size(), i = 0; i < columnCount; ++i) {
            sb.append(this.quoteName(this.columns.get(i).getName())).append(",");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    private String quoteName(final String objectName) {
        if (objectName == null || objectName.isEmpty()) {
            return "";
        }
        if (objectName.charAt(0) == '\"' && objectName.charAt(objectName.length() - 1) == '\"') {
            return this.escapeDoubleQuote(objectName);
        }
        return '\"' + this.escapeDoubleQuote(objectName) + '\"';
    }

    private String escapeDoubleQuote(final String objectName) {
        return objectName.replace("\"", "\"\"");
    }
}
