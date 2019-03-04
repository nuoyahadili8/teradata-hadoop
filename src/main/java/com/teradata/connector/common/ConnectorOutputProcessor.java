package com.teradata.connector.common;

import com.teradata.connector.common.exception.ConnectorException;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public interface ConnectorOutputProcessor {
    int outputPreProcessor(final JobContext p0) throws ConnectorException;

    int outputPostProcessor(final JobContext p0) throws ConnectorException;
}
