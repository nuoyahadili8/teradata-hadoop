package com.teradata.connector.hive.serde;

import com.teradata.connector.common.ConnectorPartitionedWritable;
import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.ConnectorSerDe;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hive.converter.HiveDataType;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;
import com.teradata.connector.hive.utils.HiveSchemaUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public abstract class HiveConnectorSerDe implements ConnectorSerDe {
    private int[] mapping;
    protected int recordLen;
    protected boolean partitioned;
    protected JobContext context;
    protected Configuration configuration;
    protected ConnectorRecord record;
    protected ConnectorRecord deserRecord;
    protected ArrayList<String> partitionValues;
    protected int nonPartSize;
    protected HiveDataType[] inputSchemaDataTypes;
    private String[] partColNames;
    private int[] partCols;
    private ConnectorPartitionedWritable partWritable;
    private HiveDataType[] outputSchemaDataTypes;
    private String lastFilePath;
    private ConnectorDataTypeConverter[] serConverter;
    private ConnectorDataTypeConverter[] deserConverter;

    public HiveConnectorSerDe() {
        this.inputSchemaDataTypes = null;
        this.partCols = null;
        this.outputSchemaDataTypes = null;
        this.lastFilePath = "";
    }

    @Override
    public void initialize(final JobContext context, final ConnectorConfiguration.direction direction) throws ConnectorException {
        this.context = context;
        this.configuration = context.getConfiguration();
        if (direction.equals(ConnectorConfiguration.direction.input)) {
            final ConnectorRecordSchema inputSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(this.configuration));
            final String[] fieldNames = HivePlugInConfiguration.getInputFieldNamesArray(this.configuration);
            final String sourcePartitionSchema = HivePlugInConfiguration.getInputPartitionSchema(this.configuration);
            final String[] schemaFieldNames = HivePlugInConfiguration.getInputTableFieldNamesArray(this.configuration);
            final String sourceTableSchema = HivePlugInConfiguration.getInputTableSchema(this.configuration).trim();
            this.inputSchemaDataTypes = HiveSchemaUtils.lookupWritableHiveDataTypes(HivePlugInConfiguration.getInputTableFieldTypes(this.configuration));
            this.recordLen = inputSchema.getLength();
            if (sourcePartitionSchema.length() > 0) {
                this.partitioned = true;
                this.partitionValues = new ArrayList<String>();
            }
            this.mapping = HiveSchemaUtils.getColumnMapping(schemaFieldNames, fieldNames);
            this.deserRecord = new ConnectorRecord(schemaFieldNames.length);
            this.nonPartSize = ConnectorSchemaUtils.parseColumns(sourceTableSchema.toLowerCase()).size();
            this.record = new ConnectorRecord(this.recordLen);
            this.deserConverter = HiveSchemaUtils.initializeHiveTypeEncoder(HivePlugInConfiguration.getInputTableSchema(this.configuration));
        } else {
            final String targetPartitionSchema = HivePlugInConfiguration.getOutputPartitionSchema(this.configuration);
            final String[] schemaField = HivePlugInConfiguration.getOutputTableFieldNamesArray(this.configuration);
            final int partitionColumnCnt = ConnectorSchemaUtils.parseColumns(targetPartitionSchema).size();
            final String[] fieldNames2 = HivePlugInConfiguration.getOutputFieldNamesArray(this.configuration);
            this.mapping = HiveSchemaUtils.getColumnMapping(schemaField, fieldNames2);
            this.nonPartSize = schemaField.length - partitionColumnCnt;
            this.record = new ConnectorRecord(this.nonPartSize);
            if (partitionColumnCnt > 0) {
                final String[] fieldName = HivePlugInConfiguration.getOutputFieldNamesArray(this.configuration);
                this.partCols = ConnectorSchemaUtils.getHivePartitionMapping(fieldName, targetPartitionSchema);
                this.partColNames = new String[this.partCols.length];
                for (int i = 0; i < this.partCols.length; ++i) {
                    this.partColNames[i] = fieldName[this.partCols[i]];
                }
                this.partitioned = true;
                this.partWritable = new ConnectorPartitionedWritable();
            }
            this.outputSchemaDataTypes = HiveSchemaUtils.lookupWritableHiveDataTypes(HivePlugInConfiguration.getOutputTableFieldTypes(this.configuration));
            this.serConverter = HiveSchemaUtils.initializeHiveTypeDecoder(HivePlugInConfiguration.getOutputTableSchema(this.configuration));
        }
    }

    @Override
    public Writable serialize(final ConnectorRecord value) throws ConnectorException {
        for (int i = 0; i < this.mapping.length; ++i) {
            if (this.mapping[i] < this.nonPartSize) {
                this.record.set(this.mapping[i], this.hiveTypeDecoder(this.mapping[i], value.get(i)));
            }
        }
        Writable result;
        try {
            result = this.doSerialize(this.record);
        } catch (SerDeException e) {
            e.printStackTrace();
            throw new ConnectorException(e.getMessage(), (Throwable) e);
        }
        if (this.partitioned) {
            this.partWritable.setPartitionPath(this.getPartitionPath(value));
            this.partWritable.setRecord(result);
            return (Writable) this.partWritable;
        }
        return result;
    }

    @Override
    public ConnectorRecord deserialize(final Writable writable) throws ConnectorException {
        if (this.partitioned) {
            final String newFilePath = this.configuration.get("map.input.file");
            if (!this.lastFilePath.equals(newFilePath)) {
                this.getPartitionValues(newFilePath);
                this.lastFilePath = newFilePath;
            }
            for (int i = 0; i < this.partitionValues.size(); ++i) {
                this.deserRecord.set(this.nonPartSize + i, this.partitionValues.get(i));
            }
        }
        try {
            this.doDeserialize(writable);
        } catch (SerDeException e) {
            throw new ConnectorException(e.getMessage(), (Throwable) e);
        }
        for (int j = 0; j < this.mapping.length; ++j) {
            this.record.set(j, this.hiveTypeEncoder(this.mapping[j], this.deserRecord.get(this.mapping[j])));
        }
        return this.record;
    }

    private void getPartitionValues(String pathString) {
        this.partitionValues.clear();
        int lastEqualPos;
        while ((lastEqualPos = pathString.indexOf(61)) >= 0) {
            pathString = pathString.substring(lastEqualPos + 1);
            final int lastSlashPos = pathString.indexOf(47);
            if (lastSlashPos < 0) {
                this.partitionValues.add(FileUtils.unescapePathName(pathString));
                break;
            }
            final String s = FileUtils.unescapePathName(pathString.substring(0, lastSlashPos));
            this.partitionValues.add(s);
            pathString = pathString.substring(lastSlashPos + 1);
        }
    }

    private String getPartitionPath(final ConnectorRecord value) throws ConnectorException {
        final List<String> partValues = new ArrayList<String>();
        final Path outputPath = FileOutputFormat.getOutputPath(this.context);
        String pathString = outputPath.toString();
        for (int i = 0; i < this.partCols.length; ++i) {
            final int colToAppend = this.partCols[i];
            final Object colValue = value.get(colToAppend);
            if (colValue == null) {
                throw new ConnectorException(15010);
            }
            final String colValueStr = FileUtils.escapePathName(ConnectorSchemaUtils.getHivePathString(colValue));
            final String partitionColName = FileUtils.escapePathName(this.partColNames[i].toLowerCase());
            partValues.add(colValueStr);
            pathString = pathString + "/" + partitionColName + "=" + colValueStr;
        }
        return pathString;
    }

    protected abstract Writable doSerialize(final ConnectorRecord p0) throws SerDeException;

    protected abstract ConnectorRecord doDeserialize(final Writable p0) throws SerDeException;

    private Object hiveTypeEncoder(final int index, final Object o) {
        final HiveDataType type = this.inputSchemaDataTypes[index];
        if (HiveSchemaUtils.isHiveComplexType(type.getType())) {
            return this.deserConverter[index].convert(o);
        }
        return o;
    }

    private Object hiveTypeDecoder(final int index, final Object o) {
        final HiveDataType type = this.outputSchemaDataTypes[index];
        if (HiveSchemaUtils.isHiveComplexType(type.getType()) || type.getType() == 3 || type.getType() == 1 || type.getType() == -2001) {
            return this.serConverter[index].convert(o);
        }
        return o;
    }
}
