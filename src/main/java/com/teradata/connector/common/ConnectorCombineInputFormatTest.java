package com.teradata.connector.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.junit.Test;

/**
 * @author Administrator
 */
public class ConnectorCombineInputFormatTest {
    @Test
    public void testGetSplits() throws Exception {
        final Configuration conf = new Configuration();
        conf.set("tdch.plugin.input.format", "org.apache.hadoop.mapreduce.lib.db.DBInputFormat");
        final JobContext context = (JobContext) new Job(conf);
        final ConnectorCombineInputFormat<LongWritable, Text> ccInputFormat = new ConnectorCombineInputFormat<LongWritable, Text>();
        ccInputFormat.plugedInInputFormat = (InputFormat<LongWritable, Writable>) new DBInputFormat();
    }
}
