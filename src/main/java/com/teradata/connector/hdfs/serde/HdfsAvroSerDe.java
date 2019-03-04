package com.teradata.connector.hdfs.serde;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.ConnectorSerDe;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hdfs.converter.HdfsAvroConverter;
import com.teradata.connector.hdfs.converter.HdfsAvroDataTypeConverter;
import com.teradata.connector.hdfs.converter.HdfsAvroDataTypeDefinition;
import com.teradata.connector.hdfs.utils.HdfsAvroSchemaUtils;
import com.teradata.connector.hdfs.utils.HdfsPlugInConfiguration;
import com.teradata.connector.hdfs.utils.HdfsSchemaUtils;

import java.nio.ByteBuffer;
import java.util.List;

import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;


/**
 * @author Administrator
 */
public class HdfsAvroSerDe implements ConnectorSerDe {
    protected Configuration configuration;
    protected ConnectorRecord sourceConnectorRecord;
    protected GenericRecord avroRecord;
    protected ObjectWritable objectWritable;
    protected GenericRecord record;
    private AvroKey<GenericRecord> key;
    protected int[] mappings;
    JobContext context;
    protected List<Schema.Field> fields;
    int sourceRecordLen;
    int targetRecordLen;
    private ConnectorDataTypeConverter[] serConverter;
    private ConnectorDataTypeConverter[] deserConverter;

    public HdfsAvroSerDe() {
        this.sourceConnectorRecord = null;
        this.objectWritable = null;
        this.key = null;
        this.mappings = null;
    }

    @Override
    public void initialize(final JobContext context, final ConnectorConfiguration.direction direction) throws ConnectorException {
        this.configuration = context.getConfiguration();
        this.context = context;
        if (direction == ConnectorConfiguration.direction.input) {
            final String[] fieldnames = HdfsPlugInConfiguration.getInputFieldNamesArray(this.configuration);
            final Schema s = new Schema.Parser().parse(HdfsPlugInConfiguration.getInputAvroSchema(this.configuration));
            if (fieldnames.length > 0) {
                this.mappings = HdfsAvroSchemaUtils.getAvroColumnMapping(s, fieldnames);
                this.sourceConnectorRecord = new ConnectorRecord(this.mappings.length);
                this.sourceRecordLen = this.mappings.length;
            } else {
                this.sourceRecordLen = s.getFields().size();
                this.sourceConnectorRecord = new ConnectorRecord(this.sourceRecordLen);
            }
            this.fields = (List<Schema.Field>) s.getFields();
            this.initializeJsonEncoder();
        } else {
            final String[] fieldnames = HdfsPlugInConfiguration.getOutputFieldNamesArray(this.configuration);
            this.objectWritable = new ObjectWritable();
            this.key = (AvroKey<GenericRecord>) new AvroKey();
            final Schema s = new Schema.Parser().parse(HdfsPlugInConfiguration.getOutputAvroSchema(this.configuration));
            final GenericRecordBuilder recordBuilder = new GenericRecordBuilder(s);
            this.fields = (List<Schema.Field>) s.getFields();
            for (int i = 0; i < this.fields.size(); ++i) {
                final Schema.Field f = this.fields.get(i);
                if (f.defaultValue() != null) {
                    recordBuilder.clear(f);
                } else if (!HdfsAvroSchemaUtils.avroSchemaIsNullable(f)) {
                    recordBuilder.set(f, HdfsAvroDataTypeDefinition.getAvroDefaultNullValue(f.schema()));
                } else {
                    recordBuilder.set(f, (Object) null);
                }
            }
            this.record = (GenericRecord) recordBuilder.build();
            this.key.datum(this.record);
            this.objectWritable.set((Object) this.key);
            if (fieldnames.length > 0) {
                this.mappings = HdfsAvroSchemaUtils.getAvroColumnMapping(s, fieldnames);
                this.targetRecordLen = this.mappings.length;
            } else {
                this.targetRecordLen = s.getFields().size();
            }
            this.initializeJsonDecoder();
        }
    }

    @Override
    public Writable serialize(final ConnectorRecord connectorRecord) throws ConnectorException {
        for (int i = 0; i < connectorRecord.getAllObject().length; ++i) {
            final Object object = connectorRecord.get(i);
            final int mIndex = (this.mappings == null) ? i : this.mappings[i];
            final Schema.Field f = this.fields.get(mIndex);
            if (object == null) {
                if (HdfsAvroSchemaUtils.avroSchemaIsNullable(f)) {
                    this.record.put(mIndex, (Object) null);
                } else {
                    this.record.put(mIndex, HdfsAvroDataTypeDefinition.getAvroDefaultNullValue(f.schema()));
                }
            } else {
                final int orgType = HdfsSchemaUtils.lookupHdfsAvroDatatype(f.schema().getType().getName());
                this.record.put(mIndex, this.avroTypeDecoder(object, orgType, i));
            }
        }
        return (Writable) this.objectWritable;
    }

    @Override
    public ConnectorRecord deserialize(final Writable writable) throws ConnectorException {
        final Object objectwritable = ((ObjectWritable) writable).get();
        this.avroRecord = (GenericRecord) ((AvroKey) objectwritable).datum();
        for (int i = 0; i < this.sourceRecordLen; ++i) {
            final int index = (this.mappings == null) ? i : this.mappings[i];
            final int orgType = HdfsSchemaUtils.lookupHdfsAvroDatatype(this.fields.get(index).schema().getType().getName());
            this.sourceConnectorRecord.set(i, this.avroTypeEncoder(this.avroRecord.get(index), orgType, i));
        }
        return this.sourceConnectorRecord;
    }

    private Object avroTypeDecoder(final Object o, final int orgType, final int index) {
        Assert.assertTrue(o != null);
        if (!HdfsAvroConverter.isComplexType(orgType)) {
            return o;
        }
        return this.serConverter[index].convert(o);
    }

    private Object avroTypeEncoder(final Object o, final int orgType, final int index) {
        if (orgType == -2) {
            return ((ByteBuffer) o).array();
        }
        if (!HdfsAvroConverter.isComplexType(orgType)) {
            return o;
        }
        return this.deserConverter[index].convert(o);
    }

    private void initializeJsonDecoder() throws ConnectorException {
        final Schema targetAvroSchema = new Schema.Parser().parse(HdfsPlugInConfiguration.getOutputAvroSchema(this.configuration));
        final ConnectorRecordSchema sourceRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getOutputConverterRecordSchema(this.configuration));
        this.serConverter = new ConnectorDataTypeConverter[sourceRecordSchema.getLength()];
        for (int i = 0; i < this.serConverter.length; ++i) {
            final int sourceType = sourceRecordSchema.getFieldType(i);
            final int index = (this.mappings != null) ? this.mappings[i] : i;
            final Schema targetSchema = targetAvroSchema.getFields().get(index).schema();
            final int targetType = HdfsAvroDataTypeDefinition.getAvroDataType(targetSchema);
            ConnectorDataTypeConverter converter = null;
            if (HdfsAvroConverter.isComplexType(targetType)) {
                if (targetType == -2005) {
                    converter = HdfsAvroConverter.lookupSimpleTypeToAvroUnionConverter(this.context, targetSchema, sourceType);
                } else if (targetType == -2000) {
                    converter = new HdfsAvroDataTypeConverter.AvroNull();
                } else {
                    converter = new HdfsAvroDataTypeConverter.JsonStringToAvroObject(targetSchema);
                }
                this.serConverter[i] = converter;
            }
        }
    }

    private void initializeJsonEncoder() throws ConnectorException {
        final Schema sourceAvroSchema = new Schema.Parser().parse(HdfsPlugInConfiguration.getInputAvroSchema(this.configuration));
        final ConnectorRecordSchema targetSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(this.configuration));
        this.deserConverter = new ConnectorDataTypeConverter[targetSchema.getLength()];
        for (int i = 0; i < this.deserConverter.length; ++i) {
            final int index = (this.mappings == null) ? i : this.mappings[i];
            final Schema sourceFieldSchema = sourceAvroSchema.getFields().get(index).schema();
            final int sourceType = HdfsAvroDataTypeDefinition.getAvroDataType(sourceFieldSchema);
            Assert.assertTrue(!HdfsAvroConverter.isComplexType(targetSchema.getFieldType(i)));
            if (HdfsAvroConverter.isComplexType(sourceType)) {
                ConnectorDataTypeConverter converter = null;
                switch (sourceType) {
                    case -2005: {
                        Assert.assertTrue(targetSchema.getFieldType(i) == 12 || targetSchema.getFieldType(i) == 1882);
                        converter = HdfsAvroConverter.lookupAvroUnionToSimpleTypeConverter(this.context, sourceFieldSchema, targetSchema.getFieldType(i));
                        break;
                    }
                    case -2006:
                    case -2004:
                    case -2003:
                    case -2002:
                    case -2001: {
                        converter = new HdfsAvroDataTypeConverter.AvroObjectToJsonString(sourceFieldSchema);
                        converter.setDefaultValue("");
                        break;
                    }
                    case -2000: {
                        converter = new HdfsAvroDataTypeConverter.AvroNull();
                        break;
                    }
                    default: {
                        throw new ConnectorException(50012);
                    }
                }
                converter.setNullable(true);
                this.deserConverter[i] = converter;
            }
        }
    }
}
