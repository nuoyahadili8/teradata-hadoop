package com.teradata.connector.hdfs.utils;

import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorStringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;


public class HdfsAvroSchemaUtils {
    private static Log logger = LogFactory.getLog((Class) HdfsAvroSchemaUtils.class);

    public static boolean avroFieldsSetDefaultOrNull(final Schema s, final String[] fieldNames) {
        final List<Schema.Field> fields = (List<Schema.Field>) s.getFields();
        for (int i = 0; i < fields.size(); ++i) {
            Schema.Field field;
            int j;
            for (field = fields.get(i), j = 0; j < fieldNames.length && !field.name().equals(fieldNames[j].trim()); ++j) {
            }
            if (j == fieldNames.length && field.defaultValue() == null && !avroSchemaIsNullable(field)) {
                return false;
            }
        }
        return true;
    }

    public static boolean avroSchemaIsNullable(final Schema.Field field) {
        if (field.schema().getType().equals((Object) Schema.Type.NULL)) {
            return true;
        }
        if (field.schema().getType().equals((Object) Schema.Type.UNION)) {
            final List<Schema> types = (List<Schema>) field.schema().getTypes();
            for (int k = 0; k < types.size(); ++k) {
                final Schema subSchema = types.get(k);
                if (subSchema.getType().equals((Object) Schema.Type.NULL)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static Schema buildAvroSchemaFromFile(final Configuration configuration, final String avroschemafile) throws ConnectorException {
        if (avroschemafile.trim().equals("")) {
            return null;
        }
        final StringBuffer sb = new StringBuffer();
        try {
            final Path filePath = new Path(avroschemafile);
            final FileSystem fs = filePath.getFileSystem(configuration);
            HdfsAvroSchemaUtils.logger.info((Object) ("Using avro schema in file : " + fs.makeQualified(filePath)));
            if (!fs.exists(filePath)) {
                throw new ConnectorException(50001);
            }
            if (!fs.isFile(filePath)) {
                throw new ConnectorException(50002);
            }
            final BufferedReader in = new BufferedReader(new InputStreamReader((InputStream) fs.open(filePath)));
            String tmp;
            while ((tmp = in.readLine()) != null) {
                sb.append(tmp + "\n");
            }
            in.close();
        } catch (IOException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        return new Schema.Parser().parse(sb.toString());
    }

    public static Schema fetchSchemaFromInputPath(final Configuration configuration) throws ConnectorException {
        final String inputDir = configuration.get("mapred.input.dir", "");
        final String[] paths = StringUtils.split(inputDir);
        final Path[] inputPath = new Path[paths.length];
        Schema s = null;
        FileSystem fileSystem = null;
        for (int i = 0; i < inputPath.length; ++i) {
            final Path path = new Path(StringUtils.unEscapeString(paths[i]));
            FileStatus[] fs;
            try {
                if (fileSystem == null) {
                    fileSystem = path.getFileSystem(configuration);
                }
                fs = fileSystem.listStatus(path);
                if (fs == null) {
                    continue;
                }
            } catch (FileNotFoundException e) {
                HdfsAvroSchemaUtils.logger.debug((Object) ConnectorStringUtils.getExceptionStack(e));
                continue;
            } catch (IOException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            }
            for (int j = 0; j < fs.length; ++j) {
                try {
                    final Path filepath = fs[j].getPath();
                    final SeekableInput si = (SeekableInput) new FsInput(filepath, configuration);
                    final DataFileReader<Object> dfr = (DataFileReader<Object>) new DataFileReader(si, (DatumReader) new ReflectDatumReader());
                    s = dfr.getSchema();
                    dfr.close();
                    si.close();
                } catch (IOException e3) {
                    HdfsAvroSchemaUtils.logger.debug((Object) ConnectorStringUtils.getExceptionStack(e3));
                    continue;
                }
                if (s != null) {
                    return s;
                }
            }
        }
        return s;
    }

    public static void checkFieldNamesInSchema(final String[] fieldNames, final Schema avroSchema) throws ConnectorException {
        final List<Schema.Field> fields = (List<Schema.Field>) avroSchema.getFields();
        final List<String> avroFields = new ArrayList<String>(fields.size());
        for (int i = 0; i < fields.size(); ++i) {
            avroFields.add(fields.get(i).name());
        }
        HdfsSchemaUtils.checkFieldNames(fieldNames, Arrays.asList(fieldNames));
    }

    public static int[] getAvroColumnMapping(final Schema avroSchema, final String[] mappingNames) throws ConnectorException {
        final List<Schema.Field> fields = (List<Schema.Field>) avroSchema.getFields();
        final List<String> avroFields = new ArrayList<String>(fields.size());
        for (int i = 0; i < fields.size(); ++i) {
            avroFields.add(fields.get(i).name());
        }
        return HdfsSchemaUtils.getColumnMapping(avroFields, mappingNames);
    }

    public static void formalizeAvroRecordSchema(final List<Schema.Field> fields, final ConnectorRecordSchema avroSchema, final int[] mapping) throws ConnectorException {
        for (int i = 0; i < avroSchema.getLength(); ++i) {
            final int index = (mapping == null) ? i : mapping[i];
            final Schema.Field field = fields.get(index);
            final int type = HdfsSchemaUtils.lookupHdfsAvroDatatype(field.schema().getType().getName());
            if (type == -2005) {
                avroSchema.setFieldType(i, 1882);
            }
        }
    }
}
