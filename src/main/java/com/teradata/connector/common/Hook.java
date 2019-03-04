package com.teradata.connector.common;

import com.teradata.connector.common.exception.ConnectorException;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Administrator
 */
public interface Hook {
    void run(final Configuration p0) throws ConnectorException;
}
