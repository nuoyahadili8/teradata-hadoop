package com.teradata.connector.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * @author Administrator
 */
public class HiveParquetReadSupportExt extends ReadSupport<ParquetExt> {
    public ReadContext init(final Configuration configuration, final Map<String, String> keyValueMetaData, final MessageType fileSchema) {
        final String partialSchemaString = configuration.get("parquet.read.schema");
        final MessageType requestedProjection = getSchemaForRead(fileSchema, partialSchemaString);
        return new ReadContext(requestedProjection);
    }

    @Override
    public RecordMaterializer<ParquetExt> prepareForRead(final Configuration configuration, final Map<String, String> keyValueMetaData, final MessageType fileSchema, final ReadContext readContext) {
        return new RecordMaterializerExt(readContext.getRequestedSchema());
    }

    class RecordMaterializerExt extends RecordMaterializer<ParquetExt> {
        private final PlainParquetFactoryExt plainParquetFactory;
        private ParquetConverterExt root;

        public RecordMaterializerExt(final MessageType schema) {
            this.plainParquetFactory = new PlainParquetFactoryExt(schema);
            this.root = new ParquetConverterExt(null, 0, schema) {
                @Override
                public void start() {
                    this.current = RecordMaterializerExt.this.plainParquetFactory.newGroup();
                }

                @Override
                public void end() {
                }
            };
        }

        @Override
        public ParquetExt getCurrentRecord() {
            return this.root.getCurrentRecord();
        }

        @Override
        public GroupConverter getRootConverter() {
            return this.root;
        }
    }

    class ParquetConverterExt extends GroupConverter {
        private final ParquetConverterExt parent;
        private final int index;
        protected ParquetExt current;
        private Converter[] converters;

        ParquetConverterExt(final ParquetConverterExt parent, final int index, final GroupType schema) {
            this.parent = parent;
            this.index = index;
            this.converters = new Converter[schema.getFieldCount()];
            for (int i = 0; i < this.converters.length; ++i) {
                final Type type = schema.getType(i);
                if (type.isPrimitive()) {
                    this.converters[i] = (Converter) new PrimitiveConverterExt(this, i);
                } else {
                    this.converters[i] = (Converter) new ParquetConverterExt(this, i, type.asGroupType());
                }
            }
        }

        @Override
        public void start() {
            this.current = this.parent.getCurrentRecord().addGroup(this.index);
        }

        @Override
        public Converter getConverter(final int fieldIndex) {
            return this.converters[fieldIndex];
        }

        @Override
        public void end() {
        }

        public ParquetExt getCurrentRecord() {
            return this.current;
        }
    }

    class PrimitiveConverterExt extends PrimitiveConverter {
        private final ParquetConverterExt parent;
        private final int index;

        PrimitiveConverterExt(final ParquetConverterExt parent, final int index) {
            this.parent = parent;
            this.index = index;
        }

        @Override
        public void addBinary(final Binary value) {
            this.parent.getCurrentRecord().add(this.index, value);
        }

        @Override
        public void addBoolean(final boolean value) {
            this.parent.getCurrentRecord().add(this.index, value);
        }

        @Override
        public void addDouble(final double value) {
            this.parent.getCurrentRecord().add(this.index, value);
        }

        @Override
        public void addFloat(final float value) {
            this.parent.getCurrentRecord().add(this.index, value);
        }

        @Override
        public void addInt(final int value) {
            this.parent.getCurrentRecord().add(this.index, value);
        }

        @Override
        public void addLong(final long value) {
            this.parent.getCurrentRecord().add(this.index, value);
        }
    }

    class PlainParquetFactoryExt extends ParquetFactoryExt {
        private final MessageType schema;

        public PlainParquetFactoryExt(final MessageType schema) {
            this.schema = schema;
        }

        @Override
        public ParquetExt newGroup() {
            return new PlainParquetExt((GroupType) this.schema);
        }
    }

    public class PlainParquetExt extends ParquetExt {
        private final GroupType schema;
        private final List<Object>[] data;

        public PlainParquetExt(final GroupType schema) {
            this.schema = schema;
            this.data = (List<Object>[]) new List[schema.getFields().size()];
            for (int i = 0; i < schema.getFieldCount(); ++i) {
                this.data[i] = new ArrayList<Object>();
            }
        }

        @Override
        public String toString() {
            return this.toString("");
        }

        public String toString(final String indent) {
            String result = "";
            int i = 0;
            for (final Type field : this.schema.getFields()) {
                final String name = field.getName();
                final List<Object> values = this.data[i];
                ++i;
                if (values != null && values.size() > 0) {
                    for (final Object value : values) {
                        result = result + indent + name;
                        if (value == null) {
                            result += ": NULL\n";
                        } else if (value instanceof ParquetExt) {
                            result = result + "\n" + ((PlainParquetExt) value).toString(indent + "  ");
                        } else {
                            result = result + ": " + value.toString() + "\n";
                        }
                    }
                }
            }
            return result;
        }

        @Override
        public ParquetExt addGroup(final int fieldIndex) {
            final PlainParquetExt g = new PlainParquetExt(this.schema.getType(fieldIndex).asGroupType());
            this.add(fieldIndex, g);
            return g;
        }

        @Override
        public ParquetExt getGroup(final int fieldIndex, final int index) {
            return (ParquetExt) this.getValue(fieldIndex, index);
        }

        private Object getValue(final int fieldIndex, final int index) {
            List<Object> list;
            try {
                list = this.data[fieldIndex];
            } catch (IndexOutOfBoundsException e) {
                throw new RuntimeException("not found " + fieldIndex + "(" + this.schema.getFieldName(fieldIndex) + ") in group:\n" + this);
            }
            if (list.size() < 1) {
                list.add(null);
            }
            try {
                return list.get(index);
            } catch (IndexOutOfBoundsException e) {
                throw new RuntimeException("not found " + fieldIndex + "(" + this.schema.getFieldName(fieldIndex) + ") element number " + index + " in group:\n" + this);
            }
        }

        private void add(final int fieldIndex, final BasicDataTypes value) {
            final Type type = this.schema.getType(fieldIndex);
            final List<Object> list = this.data[fieldIndex];
            if (!type.isRepetition(Type.Repetition.REPEATED) && !list.isEmpty()) {
                throw new IllegalStateException("field " + fieldIndex + " (" + type.getName() + ") can not have more than one value: " + list);
            }
            list.add(value);
        }

        @Override
        public int getFieldRepetitionCount(final int fieldIndex) {
            final List<Object> list = this.data[fieldIndex];
            return (list == null) ? 0 : list.size();
        }

        @Override
        public String getValueToString(final int fieldIndex, final int index) {
            return String.valueOf(this.getValue(fieldIndex, index));
        }

        @Override
        public String getString(final int fieldIndex, final int index) {
            if (this.getValue(fieldIndex, index) != null) {
                return ((ParquetBinaryValue) this.getValue(fieldIndex, index)).getString();
            }
            return null;
        }

        @Override
        public int getInteger(final int fieldIndex, final int index) {
            return ((ParquetIntegerValue) this.getValue(fieldIndex, index)).getInteger();
        }

        @Override
        public long getLong(final int fieldIndex, final int index) {
            return ((ParquetLongValue) this.getValue(fieldIndex, index)).getLong();
        }

        @Override
        public double getDouble(final int fieldIndex, final int index) {
            return ((ParquetDoubleValue) this.getValue(fieldIndex, index)).getDouble();
        }

        @Override
        public float getFloat(final int fieldIndex, final int index) {
            return ((ParquetFloatValue) this.getValue(fieldIndex, index)).getFloat();
        }

        @Override
        public boolean getBoolean(final int fieldIndex, final int index) {
            return ((ParquetBooleanValue) this.getValue(fieldIndex, index)).getBoolean();
        }

        @Override
        public Binary getBinary(final int fieldIndex, final int index) {
            return ((ParquetBinaryValue) this.getValue(fieldIndex, index)).getBinary();
        }

        public ParquetNanoTime getTimeNanos(final int fieldIndex, final int index) {
            return ParquetNanoTime.fromInt96((ParquetInt96Value) this.getValue(fieldIndex, index));
        }

        @Override
        public Binary getInt96(final int fieldIndex, final int index) {
            return ((ParquetInt96Value) this.getValue(fieldIndex, index)).getInt96();
        }

        @Override
        public void add(final int fieldIndex, final int value) {
            this.add(fieldIndex, new ParquetIntegerValue(value));
        }

        @Override
        public void add(final int fieldIndex, final long value) {
            this.add(fieldIndex, new ParquetLongValue(value));
        }

        @Override
        public void add(final int fieldIndex, final String value) {
            this.add(fieldIndex, new ParquetBinaryValue(Binary.fromString(value)));
        }

        @Override
        public void add(final int fieldIndex, final ParquetNanoTime value) {
            this.add(fieldIndex, value.toInt96());
        }

        @Override
        public void add(final int fieldIndex, final boolean value) {
            this.add(fieldIndex, new ParquetBooleanValue(value));
        }

        @Override
        public void add(final int fieldIndex, final Binary value) {
            switch (this.getType().getType(fieldIndex).asPrimitiveType().getPrimitiveTypeName()) {
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY: {
                    this.add(fieldIndex, new ParquetBinaryValue(value));
                    break;
                }
                case INT96: {
                    this.add(fieldIndex, new ParquetInt96Value(value));
                    break;
                }
                default: {
                    throw new UnsupportedOperationException(this.getType().asPrimitiveType().getName() + " not supported for Binary");
                }
            }
        }

        @Override
        public void add(final int fieldIndex, final float value) {
            this.add(fieldIndex, new ParquetFloatValue(value));
        }

        @Override
        public void add(final int fieldIndex, final double value) {
            this.add(fieldIndex, new ParquetDoubleValue(value));
        }

        @Override
        public void add(final int fieldIndex, final ParquetExt value) {
            this.data[fieldIndex].add(value);
        }

        @Override
        public GroupType getType() {
            return this.schema;
        }

        @Override
        public void writeValue(final int field, final int index, final RecordConsumer recordConsumer) {
            ((BasicDataTypes) this.getValue(field, index)).writeValue(recordConsumer);
        }
    }

    class ParquetBinaryValue extends BasicDataTypes {
        private final Binary binary;

        public ParquetBinaryValue(final Binary binary) {
            this.binary = binary;
        }

        @Override
        public Binary getBinary() {
            return this.binary;
        }

        @Override
        public String getString() {
            return this.binary.toStringUsingUTF8();
        }

        @Override
        public void writeValue(final RecordConsumer recordConsumer) {
            recordConsumer.addBinary(this.binary);
        }

        @Override
        public String toString() {
            return this.getString();
        }
    }

    class ParquetBooleanValue extends BasicDataTypes {
        private final boolean bool;

        public ParquetBooleanValue(final boolean bool) {
            this.bool = bool;
        }

        @Override
        public String toString() {
            return String.valueOf(this.bool);
        }

        @Override
        public boolean getBoolean() {
            return this.bool;
        }

        @Override
        public void writeValue(final RecordConsumer recordConsumer) {
            recordConsumer.addBoolean(this.bool);
        }
    }

    class ParquetDoubleValue extends BasicDataTypes {
        private final double value;

        public ParquetDoubleValue(final double value) {
            this.value = value;
        }

        @Override
        public double getDouble() {
            return this.value;
        }

        @Override
        public void writeValue(final RecordConsumer recordConsumer) {
            recordConsumer.addDouble(this.value);
        }

        @Override
        public String toString() {
            return String.valueOf(this.value);
        }
    }

    class ParquetFloatValue extends BasicDataTypes {
        private final float value;

        public ParquetFloatValue(final float value) {
            this.value = value;
        }

        @Override
        public float getFloat() {
            return this.value;
        }

        @Override
        public void writeValue(final RecordConsumer recordConsumer) {
            recordConsumer.addFloat(this.value);
        }

        @Override
        public String toString() {
            return String.valueOf(this.value);
        }
    }

    class ParquetIntegerValue extends BasicDataTypes {
        private final int value;

        public ParquetIntegerValue(final int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(this.value);
        }

        @Override
        public int getInteger() {
            return this.value;
        }

        @Override
        public void writeValue(final RecordConsumer recordConsumer) {
            recordConsumer.addInteger(this.value);
        }
    }

    class ParquetLongValue extends BasicDataTypes {
        private final long value;

        public ParquetLongValue(final long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(this.value);
        }

        @Override
        public long getLong() {
            return this.value;
        }

        @Override
        public void writeValue(final RecordConsumer recordConsumer) {
            recordConsumer.addLong(this.value);
        }
    }
}
