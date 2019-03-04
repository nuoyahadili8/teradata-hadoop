package com.teradata.connector.common.utils;


import com.teradata.connector.common.ConnectorPlugin;
import com.teradata.connector.common.exception.ConnectorException;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Administrator
 */
public class ConnectorPluginUtils
{
    public static void configConnectorInputPlugins(final Configuration configuration, final String pluginName) throws ConnectorException {
        final ConnectorPlugin.ConnectorSourcePlugin connector = (ConnectorPlugin.ConnectorSourcePlugin)ConnectorPlugin.getConnectorSourcePlugin(pluginName);
        ConnectorConfiguration.setPlugInInputProcessor(configuration, connector.getInputProcessor());
        ConnectorConfiguration.setPlugInInputFormat(configuration, connector.getInputFormatClass());
        ConnectorConfiguration.setInputSerDe(configuration, connector.getSerDeClass());
        ConnectorConfiguration.setPreJobHook(configuration, connector.getPreHook());
        ConnectorConfiguration.setPostJobHook(configuration, connector.getPostHook());
        connector.loadConfiguration(configuration);
    }
    
    public static void configConnectorOutputPlugins(final Configuration configuration, final String pluginName) throws ConnectorException {
        final ConnectorPlugin.ConnectorTargetPlugin connector = (ConnectorPlugin.ConnectorTargetPlugin)ConnectorPlugin.getConnectorTargetPlugin(pluginName);
        ConnectorConfiguration.setPlugInOutputProcessor(configuration, connector.getOutputProcessor());
        ConnectorConfiguration.setOutputSerDe(configuration, connector.getSerdeClass());
        ConnectorConfiguration.setPlugInOutputFormat(configuration, connector.getOutputFormatClass());
        ConnectorConfiguration.setDataConverter(configuration, connector.getConverterClass());
        ConnectorConfiguration.setPreJobHook(configuration, connector.getPreHook());
        ConnectorConfiguration.setPostJobHook(configuration, connector.getPostHook());
        connector.loadConfiguration(configuration);
    }
}
