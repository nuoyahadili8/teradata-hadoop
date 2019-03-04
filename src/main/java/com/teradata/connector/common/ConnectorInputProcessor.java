package com.teradata.connector.common;

import com.teradata.connector.common.exception.ConnectorException;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public interface ConnectorInputProcessor
{
    int inputPreProcessor(final JobContext p0) throws ConnectorException;
    
    int inputPostProcessor(final JobContext p0) throws ConnectorException;
}
