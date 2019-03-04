package com.teradata.connector.common;

import com.teradata.connector.common.exception.ConnectorException;
import org.apache.hadoop.conf.Configuration;


/**
 * @author Administrator
 */
public abstract class PreJobHook implements Hook {
    @Override
    public abstract void run(final Configuration configuration) throws ConnectorException;
}
