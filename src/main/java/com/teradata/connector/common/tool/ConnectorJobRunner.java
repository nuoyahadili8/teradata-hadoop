package com.teradata.connector.common.tool;

import com.teradata.connector.common.*;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import com.teradata.connector.teradata.processor.TeradataInternalFastExportProcessor;
import com.teradata.connector.teradata.processor.TeradataInternalFastloadProcessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;


/**
 * @author Administrator
 */
public class ConnectorJobRunner {
    private static Log logger = LogFactory.getLog((Class) ConnectorJobRunner.class);
    public static boolean jobSucceeded = false;

    public static int runJob(final Configuration configuration) throws ConnectorException {
        Job mapreduceJob;
        try {
            mapreduceJob = Job.getInstance(configuration);
        } catch (IOException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        return runJob(mapreduceJob);
    }

    public static int runJob(final Job job) throws ConnectorException {
        ConfigurationMappingUtils.hideCredentials((JobContext) job);
        ConfigurationMappingUtils.performWalletSubstitutions((JobContext) job);
        ConfigurationMappingUtils.associateCredentialsForOozieJavaAction((JobContext) job);
        ConfigurationMappingUtils.loadOozieJavaActionConf((JobContext) job);
        final Configuration configuration = job.getConfiguration();
        ConnectorInputProcessor importProcessor = null;
        ConnectorOutputProcessor exportProcessor = null;
        int result = 0;
        final String inputProcessorClass = ConnectorConfiguration.getPlugInInputProcessor(configuration);
        final String outputProcessorClass = ConnectorConfiguration.getPlugInOutputProcessor(configuration);
        ConnectorException connectorException = null;
        try {
            if (inputProcessorClass != null && inputProcessorClass.length() != 0) {
                final Constructor<?> meth = Class.forName(inputProcessorClass).getDeclaredConstructor((Class<?>[]) new Class[0]);
                importProcessor = (ConnectorInputProcessor) meth.newInstance(new Object[0]);
            }
            if (outputProcessorClass != null && outputProcessorClass.length() != 0) {
                final Constructor<?> meth = Class.forName(outputProcessorClass).getDeclaredConstructor((Class<?>[]) new Class[0]);
                exportProcessor = (ConnectorOutputProcessor) meth.newInstance(new Object[0]);
            }
            if (ConnectorConfiguration.getThrottleNumMappers(configuration)) {
                HadoopConfigurationUtils.utilizeMaxConcurrentMappers((JobContext) job);
            }
            if (importProcessor != null) {
                result = importProcessor.inputPreProcessor((JobContext) job);
            }
            if (result == 1001) {
                return 0;
            }
            if (result != 0) {
                return result;
            }
            if (exportProcessor != null) {
                result = exportProcessor.outputPreProcessor((JobContext) job);
            }
            if (result != 0) {
                return result;
            }
            final boolean jobValid = validatePlugedIn(configuration);
            if (!jobValid) {
                throw new ConnectorException(15011);
            }
            configureJob(job);
            result = (job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            result = 1;
            connectorException = new ConnectorException(e.getMessage(), e);
            throw connectorException;
        } finally {
            try {
                if (result == 1) {
                    synchronized (TeradataInternalFastloadProcessor.class) {
                        TeradataInternalFastloadProcessor.jobSuccess = false;
                    }
                    synchronized (TeradataInternalFastExportProcessor.class) {
                        TeradataInternalFastExportProcessor.jobSuccess = false;
                    }
                    ConnectorConfiguration.setJobSucceeded(configuration, false);
                }
                if (result != 1001) {
                    int postProcessorResult = 0;
                    if (exportProcessor != null) {
                        postProcessorResult = exportProcessor.outputPostProcessor((JobContext) job);
                    }
                    if (postProcessorResult != 0) {
                        logger.warn((Object) new ConnectorException("The output post processor returns " + postProcessorResult));
                    }
                    if (importProcessor != null) {
                        postProcessorResult = importProcessor.inputPostProcessor((JobContext) job);
                    }
                    if (postProcessorResult != 0) {
                        logger.warn((Object) new ConnectorException("The input post processor returns " + postProcessorResult));
                    }
                }
                try {
                    final List<Hook> postJobHooks = getJobHooks(job.getConfiguration(), PostJobHook.class);
                    for (final Hook hook : postJobHooks) {
                        hook.run(configuration);
                    }
                } catch (Exception e2) {
                    result = 1;
                    e2.printStackTrace();
                }
            } catch (Throwable e3) {
                if (null != connectorException) {
                    throw connectorException;
                }
                if (e3 instanceof ConnectorException) {
                    throw (ConnectorException) e3;
                }
                if (e3 instanceof RuntimeException) {
                    throw (RuntimeException) e3;
                }
                throw new ConnectorException(e3.getMessage(), e3);
            }
        }
        return result;
    }

    protected static Boolean validatePlugedIn(final Configuration configuration) {
        final String plugedInInputFormat = ConnectorConfiguration.getPlugInInputFormat(configuration);
        final String plugedInOutputFormat = ConnectorConfiguration.getPlugInOutputFormat(configuration);
        final String sourceSerDe = ConnectorConfiguration.getInputSerDe(configuration);
        final String targetSerDe = ConnectorConfiguration.getOutputSerDe(configuration);
        if (plugedInInputFormat.isEmpty()) {
            return false;
        }
        if (plugedInOutputFormat.isEmpty()) {
            return false;
        }
        if (sourceSerDe.isEmpty()) {
            return false;
        }
        if (targetSerDe.isEmpty()) {
            return false;
        }
        if (logger.isDebugEnabled()) {
            logger.debug((Object) ("plugedIn InputFormat is " + plugedInInputFormat));
            logger.debug((Object) ("plugedIn OutputFormat is " + plugedInOutputFormat));
        }
        return true;
    }

    protected static void configureJob(final Job job) throws ConnectorException {
        final Configuration configuration = job.getConfiguration();
        try {
            final List<Hook> preJobHooks = getJobHooks(job.getConfiguration(), PreJobHook.class);
            for (final Hook hook : preJobHooks) {
                hook.run(configuration);
            }
        } catch (Exception e) {
            logger.warn((Object) e);
        }
        boolean useCombinedInputFormat = ConnectorConfiguration.getUseCombinedInputFormat(configuration);
        if (!useCombinedInputFormat) {
            try {
                final InputFormat<?, ?> plugedInInputFormat = (InputFormat<?, ?>) Class.forName(ConnectorConfiguration.getPlugInInputFormat(configuration)).newInstance();
                useCombinedInputFormat = (plugedInInputFormat.getSplits((JobContext) job).size() > ConnectorConfiguration.getNumMappers(configuration));
            } catch (InstantiationException e2) {
                logger.warn((Object) e2);
            } catch (IllegalAccessException e3) {
                logger.warn((Object) e3);
            } catch (ClassNotFoundException e4) {
                logger.warn((Object) e4);
            } catch (IOException e5) {
                throw new ConnectorException(e5.getMessage(), e5);
            } catch (InterruptedException e6) {
                logger.warn((Object) e6);
            }
        }
        final boolean usePartitionedOutputFormat = ConnectorConfiguration.getUsePartitionedOutputFormat(configuration);
        job.setJarByClass((Class) ConnectorJobRunner.class);
        if (!useCombinedInputFormat) {
            job.setInputFormatClass((Class) ConnectorInputFormat.class);
        } else {
            job.setInputFormatClass((Class) ConnectorCombineInputFormat.class);
        }
        if (!usePartitionedOutputFormat) {
            job.setOutputFormatClass((Class) ConnectorOutputFormat.class);
        } else {
            job.setOutputFormatClass((Class) ConnectorPartitionedOutputFormat.class);
        }
        final int numReducers = ConnectorConfiguration.getNumReducers(configuration);
        if (numReducers == 0) {
            job.setOutputKeyClass((Class) NullWritable.class);
            job.setMapperClass((Class) ConnectorMMapper.class);
            job.setNumReduceTasks(0);
        } else {
            job.setMapperClass((Class) ConnectorMRMapper.class);
            job.setReducerClass((Class) ConnectorReducer.class);
            job.setNumReduceTasks(numReducers);
            if (configuration.getClass("mapred.mapoutput.value.class", (Class) null, (Class) Object.class) == null) {
                job.setMapOutputValueClass((Class) ConnectorRecord.class);
            }
        }
        job.setSpeculativeExecution(false);
        if (logger.isDebugEnabled()) {
            if (configuration.getBoolean("mapred.map.tasks.speculative.execution", false) || configuration.getBoolean("mapreduce.map.speculative", false)) {
                logger.debug((Object) "speculative configuration is true");
            } else {
                logger.debug((Object) "speculative configuration is false");
            }
            logger.debug((Object) ("scheduler class is " + configuration.get("mapred.jobtracker.taskScheduler", "")));
        }
    }

    private static <T> List<Hook> getJobHooks(final Configuration configuration, final Class<T> clazz) throws Exception {
        final List<Hook> hooks = new ArrayList<Hook>();
        String csHooks;
        if (clazz.isAssignableFrom(PreJobHook.class)) {
            csHooks = ConnectorConfiguration.getPreJobHook(configuration);
        } else {
            csHooks = ConnectorConfiguration.getPostJobHook(configuration);
        }
        if (csHooks == null) {
            return hooks;
        }
        csHooks = csHooks.trim();
        if (csHooks.equals("")) {
            return hooks;
        }
        final String[] split;
        final String[] hookClasses = split = csHooks.split(",");
        for (final String hookClass : split) {
            try {
                final Hook hook = (Hook) Class.forName(hookClass.trim()).newInstance();
                hooks.add(hook);
            } catch (ClassNotFoundException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
        return hooks;
    }
}
