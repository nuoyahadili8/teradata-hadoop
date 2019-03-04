package com.teradata.connector.hive;

import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;
import com.teradata.connector.hive.utils.HiveUtils;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.api.WriteSupport;


/**
 * @author Administrator
 */
public class HiveParquetFileOutputFormat extends FileOutputFormat<Void, Writable> {
    boolean useApacheVerflag;

    public HiveParquetFileOutputFormat() {
        this.useApacheVerflag = true;
    }

    @Override
    public RecordWriter getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        String outputPath = null;
        final String outputdir = context.getConfiguration().get("mapred.output.dir");
        final String partition = FileOutputFormat.getUniqueFile(context, HadoopConfigurationUtils.getOutputBaseName((JobContext) context), "");
        if (outputdir != null) {
            outputPath = outputdir + "/" + partition;
        } else {
            outputPath = partition;
        }
        final String targetTableSchema = HivePlugInConfiguration.getOutputTableSchema(context.getConfiguration());
        final List<String> columns = ConnectorSchemaUtils.parseColumns(targetTableSchema.toLowerCase());
        final List<String> columnNames = ConnectorSchemaUtils.parseColumnNames(columns);
        final List<String> columnTypes = ConnectorSchemaUtils.parseColumnTypes(columns);
        final Properties props = new Properties();
        props.setProperty("columns", HiveUtils.listToString(columnNames));
        props.setProperty("columns.types", HiveUtils.listToString(columnTypes));
        final List<TypeInfo> columnTypeInfos = (List<TypeInfo>) TypeInfoUtils.getTypeInfosFromTypeString(HiveUtils.listToString(columnTypes));
        final ParquetOutputFormatExt pOutExt = new ParquetOutputFormatExt();
        final Object output = pOutExt.getParquetOutputFormat((WriteSupport) this.returnWriteSupport((WriteSupport) new DataWritableWriteSupport()));
        DataWritableWriteSupport.setSchema(HiveSchemaConverter.convert((List) columnNames, (List) columnTypeInfos), conf);
        this.initializeSerProperties(conf, props);
        final RecordWriter writer = this.useApacheVerflag ? ((ParquetOutputFormat) output).getRecordWriter(context, new Path(outputPath)) : ((org.apache.parquet.hadoop.ParquetOutputFormat) output).getRecordWriter(context, new Path(outputPath));
        return new RecordWriter<NullWritable, Writable>() {
            @Override
            public void write(final NullWritable key, final Writable value) throws IOException, InterruptedException {
                writer.write((Object) null, (Object) value);
            }

            @Override
            public void close(final TaskAttemptContext arg0) throws IOException, InterruptedException {
                writer.close(arg0);
            }
        };
    }

    public DataWritableWriteSupport returnWriteSupport(final WriteSupport ws) {
        this.useApacheVerflag = true;
        return new DataWritableWriteSupport();
    }

    public DataWritableWriteSupport returnWriteSupport(final org.apache.parquet.hadoop.api.WriteSupport ws) {
        this.useApacheVerflag = false;
        return new DataWritableWriteSupport();
    }

    private void initializeSerProperties(final Configuration conf, final Properties tableProperties) {
        final String blockSize = tableProperties.getProperty("parquet.block.size");
        if (blockSize != null && !blockSize.isEmpty()) {
            conf.setInt("parquet.block.size", (int) Integer.valueOf(blockSize));
        }
        final String enableDictionaryPage = tableProperties.getProperty("parquet.enable.dictionary");
        if (enableDictionaryPage != null && !enableDictionaryPage.isEmpty()) {
            conf.setBoolean("parquet.enable.dictionary", (boolean) Boolean.valueOf(enableDictionaryPage));
        }
        final String compressionName = tableProperties.getProperty("parquet.compression");
        if (compressionName != null && !compressionName.isEmpty()) {
            conf.set("parquet.compression", compressionName.toUpperCase());
        }
    }
}
