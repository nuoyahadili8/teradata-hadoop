package com.teradata.connector.common;

import com.teradata.connector.common.exception.ConnectorException;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Administrator
 */
public abstract class PostJobHook implements Hook {
    @Override
    public abstract void run(final Configuration p0) throws ConnectorException;
}
