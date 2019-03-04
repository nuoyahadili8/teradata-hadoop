package com.teradata.connector.common;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public interface ConnectorSerDe {
    void initialize(final JobContext p0, final ConnectorConfiguration.direction p1) throws ConnectorException;

    Writable serialize(final ConnectorRecord p0) throws ConnectorException;

    ConnectorRecord deserialize(final Writable p0) throws ConnectorException;
}
