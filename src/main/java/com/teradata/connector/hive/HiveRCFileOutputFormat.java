package com.teradata.connector.hive;

import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * @author Administrator
 */
public class HiveRCFileOutputFormat<K, V> extends FileOutputFormat<WritableComparable<LongWritable>, BytesRefArrayWritable> {
    public static void setColumnNumber(final Configuration configuration, final int columnNum) {
        assert columnNum > 0;
        configuration.setInt(RCFile.COLUMN_NUMBER_CONF_STR, columnNum);
    }

    public static int getColumnNumber(final Configuration configuration) {
        return configuration.getInt(RCFile.COLUMN_NUMBER_CONF_STR, 0);
    }

    @Override
    public RecordWriter<WritableComparable<LongWritable>, BytesRefArrayWritable> getRecordWriter(final TaskAttemptContext arg0) throws IOException, InterruptedException {
        final Configuration configuration = arg0.getConfiguration();
        configuration.setBoolean("mapred.output.compress", true);
        final Path outputPath = FileOutputFormat.getOutputPath((JobContext) arg0);
        final FileSystem fs = outputPath.getFileSystem(configuration);
        if (!fs.exists(outputPath)) {
            fs.mkdirs(outputPath);
        }
        final Path file = this.getDefaultWorkFile(arg0, "");
        CompressionCodec codec = null;
        if (getCompressOutput((JobContext) arg0)) {
            final Class<?> codecClass = (Class<?>) getOutputCompressorClass((JobContext) arg0, (Class) DefaultCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance((Class) codecClass, configuration);
        }
        if (getColumnNumber(configuration) == 0) {
            final String targetSchema = HivePlugInConfiguration.getOutputTableSchema(configuration);
            setColumnNumber(configuration, ConnectorSchemaUtils.parseColumnNames(ConnectorSchemaUtils.parseColumns(targetSchema)).size());
        }
        final RCFile.Writer out = new RCFile.Writer(fs, configuration, file, (Progressable) null, codec);
        return new RecordWriter<WritableComparable<LongWritable>, BytesRefArrayWritable>() {
            @Override
            public void write(final WritableComparable<LongWritable> key, final BytesRefArrayWritable value) throws IOException {
                out.append(value);
            }

            @Override
            public void close(final TaskAttemptContext arg0) throws IOException, InterruptedException {
                out.close();
            }
        };
    }

    @Override
    public Path getDefaultWorkFile(final TaskAttemptContext context, final String extension) throws IOException {
        return new Path(context.getConfiguration().get("mapred.output.dir"), FileOutputFormat.getUniqueFile(context, HadoopConfigurationUtils.getOutputBaseName((JobContext) context), extension));
    }
}
