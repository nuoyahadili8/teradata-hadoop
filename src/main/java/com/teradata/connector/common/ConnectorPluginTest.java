package com.teradata.connector.common;

import org.apache.hadoop.conf.*;
import com.teradata.connector.common.exception.*;
import org.junit.Assert;
import org.junit.Test;

public class ConnectorPluginTest {
    @Test
    public void testLoadConnectorPluginFromConf() throws Exception {
        final Configuration conf = new Configuration();
        conf.set("tdch.job.plugin.configuration.file", "file.conf");
        try {
            ConnectorPlugin.loadConnectorPluginFromConf(conf);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("file does not exist :file.conf"));
        }
    }

    @Test
    public void testGetConnectorSourcePlugin() throws Exception {
        try {
            ConnectorPlugin.getConnectorSourcePlugin("unknown");
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("plugin \"unknown\" not found"));
        }
        Assert.assertTrue(ConnectorPlugin.getConnectorSourcePlugin("teradata-split.by.amp") instanceof ConnectorPlugin);
    }

    @Test
    public void testGetConnectorTargetPlugin() throws Exception {
        try {
            ConnectorPlugin.getConnectorTargetPlugin("unknown");
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("plugin \"unknown\" not found"));
        }
        Assert.assertTrue(ConnectorPlugin.getConnectorTargetPlugin("hive-orcfile") instanceof ConnectorPlugin);
    }

    @Test
    public void testValidate() throws Exception {
        final ConnectorPlugin.ConnectorSourcePlugin csp = new ConnectorPlugin.ConnectorSourcePlugin();
        csp.setInputFormatClass(null);
        csp.setConfigurationClass(null);
        csp.setSerDeClass(null);
        csp.setInputProcessor(null);
        try {
            csp.validate();
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("bad configuration"));
        }
        final ConnectorPlugin.ConnectorTargetPlugin ctp = new ConnectorPlugin.ConnectorTargetPlugin();
        ctp.setOutputFormatClass(null);
        ctp.setConfigurationClass(null);
        ctp.setOutputProcessor(null);
        try {
            ctp.validate();
        } catch (ConnectorException ce2) {
            Assert.assertTrue(ce2.getMessage().contains("bad configuration"));
        }
    }

    @Test
    public void testToString() throws Exception {
        final ConnectorPlugin.ConnectorSourcePlugin csp = new ConnectorPlugin.ConnectorSourcePlugin();
        final ConnectorPlugin.ConnectorTargetPlugin ctp = new ConnectorPlugin.ConnectorTargetPlugin();
        csp.setInputFormatClass("Input");
        ctp.setOutputFormatClass("Output");
        Assert.assertTrue(csp.toString().contains("inputFormatClass: Input"));
        Assert.assertTrue(ctp.toString().contains("outputFormatClass: Output"));
    }
}
