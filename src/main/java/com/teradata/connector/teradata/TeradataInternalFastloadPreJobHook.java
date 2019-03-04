package com.teradata.connector.teradata;

import com.teradata.connector.common.PreJobHook;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.hcat.ConnectorCombineFileHCatInputFormat;
import org.apache.hadoop.conf.Configuration;


public class TeradataInternalFastloadPreJobHook extends PreJobHook {
    @Override
    public void run(final Configuration configuration) throws ConnectorException {
        try {
            if (Class.forName(ConnectorConfiguration.getPlugInInputFormat(configuration)).isAssignableFrom(ConnectorCombineFileHCatInputFormat.class)) {
                ConnectorConfiguration.setUseCombinedInputFormat(configuration, false);
            } else {
                ConnectorConfiguration.setUseCombinedInputFormat(configuration, true);
            }
        } catch (ClassNotFoundException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }
}
