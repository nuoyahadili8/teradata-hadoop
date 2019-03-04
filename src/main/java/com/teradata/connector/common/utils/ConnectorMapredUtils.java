package com.teradata.connector.common.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.lang.reflect.Constructor;

/**
 * @author Administrator
 */
public class ConnectorMapredUtils {

    public static TaskAttemptContext createTaskAttemptContext(final Configuration configuration, final TaskAttemptContext taskContext) {
        try {
            Constructor<?> ctor = null;
            if (TaskAttemptContext.class.getConstructors().length == 0) {
                final Class<?> c = Class.forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
                final Class<?> taid = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptID");
                ctor = c.getConstructor(Configuration.class, taid);
            } else {
                ctor = TaskAttemptContext.class.getConstructor(Configuration.class, TaskAttemptID.class);
            }
            return (TaskAttemptContext) ctor.newInstance(configuration, taskContext.getTaskAttemptID());
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static JobContext createJobContext(final TaskAttemptContext taskContext) {
        try {
            Constructor<?> ctor = null;
            if (JobContext.class.getConstructors().length == 0) {
                final Class<?> c = Class.forName("org.apache.hadoop.mapreduce.task.JobContextImpl");
                final Class<?> taid = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptID");
                ctor = c.getConstructor(Configuration.class, taid);
            } else {
                ctor = JobContext.class.getConstructor(Configuration.class, TaskAttemptID.class);
            }
            return (JobContext) ctor.newInstance(taskContext.getConfiguration(), taskContext.getTaskAttemptID());
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
