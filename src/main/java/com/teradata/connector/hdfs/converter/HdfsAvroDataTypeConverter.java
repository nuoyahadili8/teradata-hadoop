package com.teradata.connector.hdfs.converter;

import com.teradata.connector.common.converter.ConnectorDataTypeConverter;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BinaryToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BlobToBinary;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.ClobToString;
import com.teradata.connector.common.utils.ConnectorStringUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;


public abstract class HdfsAvroDataTypeConverter extends ConnectorDataTypeConverter {

    public static final class AvroBinaryToBytes extends HdfsAvroDataTypeConverter {
        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : HdfsAvroDataTypeDefinition.BYTE_NULL_VALUE;
            } else {
                return ((ByteBuffer) object).array();
            }
        }
    }

    public static final class AvroBinaryToString extends HdfsAvroDataTypeConverter {
        private BinaryToString bts = new BinaryToString();

        @Override
        public Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : "";
            } else {
                return this.bts.convert(((ByteBuffer) object).array());
            }
        }
    }

    public static final class AvroNull extends HdfsAvroDataTypeConverter {
        @Override
        public Object convert(Object object) {
            return null;
        }
    }

    public static final class AvroObjectToJsonString extends HdfsAvroDataTypeConverter {
        private JsonEncoder e;
        private Schema fieldSchema;
        private ByteArrayOutputStream outstream = new ByteArrayOutputStream();
        private GenericDatumWriter<Object> w;

        public AvroObjectToJsonString(Schema s) {
            this.fieldSchema = s;
            this.w = new GenericDatumWriter(s);
            try {
                this.e = EncoderFactory.get().jsonEncoder(this.fieldSchema, this.outstream);
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        @Override
        public Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : "";
            } else {
                this.outstream.reset();
                try {
                    this.w.write(object, this.e);
                    this.e.flush();
                    return this.outstream.toString();
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }
    }

    public static final class AvroUnionToSimpleType extends HdfsAvroDataTypeConverter {
        private ConnectorDataTypeConverter[] converters;

        public void setConverter(ConnectorDataTypeConverter[] c) {
            this.converters = c;
        }

        @Override
        public Object convert(Object object) {
            Object o = null;
            boolean canCast = false;
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < this.converters.length; i++) {
                if (this.converters[i] != null) {
                    try {
                        o = this.converters[i].convert(object);
                        canCast = true;
                        if (o != null) {
                            break;
                        }
                    } catch (Exception e) {
                        sb.append(ConnectorStringUtils.getExceptionStack(e));
                        sb.append("\n");
                        o = null;
                    }
                }
            }
            if (!canCast) {
                throw new RuntimeException(sb.toString());
            }
            return o;
        }
    }

    public static final class BlobToAvroBinary extends HdfsAvroDataTypeConverter {
        private BlobToBinary blobToBytes = new BlobToBinary();

        @Override
        public final Object convert(final Object object) {
            if (object == null) {
                return null;
            }
            final byte[] bytes = (byte[])this.blobToBytes.convert(object);
            if (bytes.length == 0) {
                return ByteBuffer.allocate(0);
            }
            final ByteBuffer bb = ByteBuffer.allocate(bytes.length);
            System.arraycopy(bytes, 0, bb.array(), 0, bytes.length);
            return bb;
        }
    }

    public static final class ByteArrayToAvroBinary extends ConnectorDataTypeConverter {
        @Override
        public Object convert(final Object object) {
            if (object == null) {
                return ByteBuffer.allocate(0);
            }
            final byte[] bytes = (byte[])object;
            final ByteBuffer bb = ByteBuffer.allocate(bytes.length);
            System.arraycopy(bytes, 0, bb.array(), 0, bytes.length);
            return bb;
        }
    }

    public static final class ClobToAvroObject extends HdfsAvroDataTypeConverter {
        private ClobToString clobToStr = new ClobToString();
        private JsonStringToAvroObject jSonToAvro;

        public ClobToAvroObject(Schema s) {
            this.jSonToAvro = new JsonStringToAvroObject(s);
        }

        @Override
        public Object convert(Object object) {
            if (object == null) {
                return null;
            }
            return this.jSonToAvro.convert((String) this.clobToStr.convert(object));
        }
    }

    public static final class JsonStringToAvroObject extends HdfsAvroDataTypeConverter {
        private JsonDecoder e2;
        private Schema fieldSchema;
        private GenericDatumReader<Object> r;

        public JsonStringToAvroObject(Schema s) {
            this.fieldSchema = s;
            this.r = new GenericDatumReader(s);
            try {
                this.e2 = DecoderFactory.get().jsonDecoder(this.fieldSchema, "");
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        @Override
        public Object convert(Object object) {
            if (object == null) {
                return null;
            }
            try {
                this.e2.configure(object.toString());
                return this.r.read(null, this.e2);
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    public static final class NonStringToAvroUnion extends HdfsAvroDataTypeConverter {
        private ConnectorDataTypeConverter[] converters;

        public void setConverter(ConnectorDataTypeConverter[] c) {
            this.converters = c;
        }

        @Override
        public Object convert(Object object) {
            if (object == null) {
                return null;
            }
            Object o = null;
            boolean canCast = false;
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < this.converters.length; i++) {
                if (this.converters[i] != null) {
                    try {
                        o = this.converters[i].convert(object);
                        canCast = true;
                        if (o != null) {
                            return o;
                        }
                    } catch (Exception e) {
                        sb.append(ConnectorStringUtils.getExceptionStack(e));
                        sb.append("\n");
                        o = null;
                    }
                }
            }
            if (canCast) {
                return o;
            }
            throw new RuntimeException(sb.toString());
        }
    }
}
