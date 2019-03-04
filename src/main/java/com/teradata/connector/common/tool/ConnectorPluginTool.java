package com.teradata.connector.common.tool;

import com.teradata.connector.common.ConnectorPlugin;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @author Administrator
 */
public class ConnectorPluginTool extends Configured implements Tool {
    private static Log logger;

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration configuration = this.getConf();
        final Job job = new Job(configuration);
        try {
            if (this.processArgs(configuration, args) < 0) {
                this.printHelp();
                return 0;
            }
        } catch (ConnectorException e) {
            this.printHelp();
            throw e;
        }
        return ConnectorJobRunner.runJob(job);
    }

    private void printHelp() {
        System.out.println("hadoop jar teradata-hadoop-connector.jar");
        System.out.println("     com.teradata.connector.common.tool.ConnectorPluginTool");
        System.out.println("     [-pluginconf <conf file>] (optional)");
        System.out.println("     [-sourceplugin <name>] (required, the name of source plugin)");
        System.out.println("     [-targetplugin <name>] (required, the name of target plugin)");
        System.out.println("     [-sourcerecordschema <record schema>] (optional, comma delimited format)");
        System.out.println("     [-targetrecordschema <record schema>] (optional, comma delimited format)");
        System.out.println("     [-nummappers <num>] (optional, default is 2)");
        System.out.println("     [-writephaseclose <true|false>] (optional, a debug option to close write stage of the whole hadoop job)");
        System.out.println("    [-h|help] (optional)");
        System.out.println("");
    }

    public int processArgs(final Configuration configuration, final String[] args) throws ConnectorException {
        final Map<String, String> configurationMap = new HashMap<String, String>();
        int i = 0;
        final int length = args.length;
        if (length < 1) {
            throw new ConnectorException(12021);
        }
        while (i < length) {
            final String arg = args[i];
            if (arg == null || arg.isEmpty()) {
                return 0;
            }
            if (arg.charAt(0) != '-') {
                throw new ConnectorException(12001);
            }
            if (arg.equalsIgnoreCase("-h") || arg.equalsIgnoreCase("-help")) {
                return -1;
            }
            if (++i >= length) {
                throw new ConnectorException(12001);
            }
            final String value = args[i];
            if (value == null) {
                throw new ConnectorException(12001);
            }
            configurationMap.put(arg, value);
            ++i;
        }
        if (configurationMap.containsKey("-pluginconf")) {
            ConnectorConfiguration.setPluginConf(configuration, configurationMap.remove("-pluginconf"));
            try {
                ConnectorPlugin.loadConnectorPluginFromConf(configuration);
            } catch (Exception e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
        String sourcePlugin = "";
        if (configurationMap.containsKey("-sourceplugin")) {
            sourcePlugin = configurationMap.remove("-sourceplugin");
        }
        String targetPlugin = "";
        if (configurationMap.containsKey("-targetplugin")) {
            targetPlugin = configurationMap.remove("-targetplugin");
        }
        if (sourcePlugin.trim().equals("") || targetPlugin.trim().equals("")) {
            throw new ConnectorException(15026);
        }
        ConnectorPluginUtils.configConnectorInputPlugins(configuration, sourcePlugin);
        ConnectorPluginUtils.configConnectorOutputPlugins(configuration, targetPlugin);
        if (configurationMap.containsKey("-nummappers")) {
            try {
                final int numMappers = Integer.parseInt(configurationMap.remove("-nummappers"));
                if (numMappers < 0) {
                    throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-nummappers", " a value equals or more than zero "));
                }
                ConnectorConfiguration.setNumMappers(configuration, numMappers);
            } catch (NumberFormatException e2) {
                ConnectorPluginTool.logger.info((Object) String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-nummappers", "integer"));
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-nummappers", "integer"), e2);
            }
        }
        if (configurationMap.containsKey("-sourcerecordschema")) {
            final String sourceRecordSchema = configurationMap.remove("-sourcerecordschema");
            final ConnectorSchemaParser parser = new ConnectorSchemaParser();
            final List<String> schemalist = parser.tokenize(sourceRecordSchema);
            final ConnectorRecordSchema recordschema = new ConnectorRecordSchema(schemalist.size());
            int index = 0;
            for (final String schema : schemalist) {
                recordschema.setFieldType(index, ConnectorSchemaUtils.lookupDataTypeAndValidate(schema));
                if (ConnectorSchemaUtils.lookupDataTypeAndValidate(schema) == 1883) {
                    final int position = schema.indexOf(40);
                    if (position == -1) {
                        throw new ConnectorException(15013);
                    }
                    recordschema.setDataTypeConverter(index, schema.substring(0, position));
                    final String[] parameters = ConnectorSchemaUtils.getUdfParameters(schema);
                    final String[] newparameter = new String[parameters.length];
                    int pos = 0;
                    for (final String parameter : parameters) {
                        newparameter[pos++] = configuration.get(parameter, "");
                    }
                    recordschema.setParameters(index, newparameter);
                }
                ++index;
            }
            ConnectorConfiguration.setInputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(recordschema)));
            ConnectorPluginTool.logger.info((Object) ("source record schema is " + sourceRecordSchema));
        }
        if (configurationMap.containsKey("-targetrecordschema")) {
            final String targetRecordSchema = configurationMap.remove("-targetrecordschema");
            final ConnectorSchemaParser parser = new ConnectorSchemaParser();
            final List<String> schemalist = parser.tokenize(targetRecordSchema);
            final ConnectorRecordSchema recordschema = new ConnectorRecordSchema(schemalist.size());
            int index = 0;
            for (final String schema : schemalist) {
                recordschema.setFieldType(index, ConnectorSchemaUtils.lookupDataTypeAndValidate(schema));
                if (ConnectorSchemaUtils.lookupDataTypeAndValidate(schema) == 1883) {
                    final int position = schema.indexOf(40);
                    if (position == -1) {
                        throw new ConnectorException(15013);
                    }
                    recordschema.setDataTypeConverter(index, schema.substring(0, position));
                    final String[] parameters = ConnectorSchemaUtils.getUdfParameters(schema);
                    final String[] newparameter = new String[parameters.length];
                    int pos = 0;
                    for (final String parameter : parameters) {
                        newparameter[pos++] = configuration.get(parameter, "");
                    }
                    recordschema.setParameters(index, newparameter);
                }
                ++index;
            }
            ConnectorConfiguration.setOutputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(recordschema)));
            ConnectorPluginTool.logger.info((Object) ("target record schema is " + targetRecordSchema));
        }
        if (configurationMap.containsKey("-debugoption")) {
            final int debugOption = Integer.parseInt(configurationMap.remove("-debugoption"));
            ConnectorConfiguration.setDebugOption(configuration, debugOption);
        }
        if (!configurationMap.isEmpty()) {
            String unrecognizedParams = "";
            for (final String key : configurationMap.keySet()) {
                unrecognizedParams = unrecognizedParams + key + " ";
            }
            throw new ConnectorException(15008, new Object[]{unrecognizedParams});
        }
        return 0;
    }

    public static void main(final String[] args) {
        int res = 1;
        try {
            final long start = System.currentTimeMillis();
            ConnectorPluginTool.logger.info((Object) ("ConnectorPluginTool starts at " + start));
            res = ToolRunner.run((Tool) new ConnectorPluginTool(), args);
            final long end = System.currentTimeMillis();
            ConnectorPluginTool.logger.info((Object) ("ConnectorPluginTool ends at " + end));
            ConnectorPluginTool.logger.info((Object) ("ConnectorPluginTool time is " + (end - start) / 1000L + "s"));
        } catch (Throwable e) {
            ConnectorPluginTool.logger.info((Object) ConnectorStringUtils.getExceptionStack(e));
            if (e instanceof ConnectorException) {
                res = ((ConnectorException) e).getCode();
            } else {
                res = 10000;
            }
        } finally {
            ConnectorPluginTool.logger.info((Object) ("job completed with exit code " + res));
            if (res > 255) {
                res /= 1000;
            }
            System.exit(res);
        }
    }

    static {
        ConnectorPluginTool.logger = LogFactory.getLog((Class) ConnectorPluginTool.class);
    }
}
