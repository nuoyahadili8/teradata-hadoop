package com.teradata.connector.common;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

/**
 * @author Administrator
 */
public abstract class ConnectorPlugin {
    private static HashMap<String, ConnectorPlugin> sourcePlugins;
    private static HashMap<String, ConnectorPlugin> targetPlugins;
    private static final String CONNECTOR_PLUGINS_FILE = "teradata.connector.plugins.xml";
    private static Log logger;
    public static final String DUMMY_TERA_MODULE_TYPE = "teradata";
    public static final String TAG_PLUGINS = "plugins";
    public static final String TAG_SOURCE = "source";
    public static final String TAG_TARGET = "target";
    public static final String TAG_NAME = "name";
    public static final String TAG_DESC = "description";
    public static final String TAG_CONF_CLZ = "configurationClass";
    public static final String TAG_INPUT_FORMAT_CLZ = "inputformatClass";
    public static final String TAG_SERDE_CLZ = "serdeClass";
    public static final String TAG_CONVERTER_CLZ = "converterClass";
    public static final String TAG_INPUT_PROC = "inputProcessor";
    public static final String TAG_PROPERTIES = "properties";
    public static final String TAG_PROPERTY = "property";
    public static final String TAG_VALUE = "value";
    public static final String TAG_OUTPUT_FORMAT_CLZ = "outputformatClass";
    public static final String TAG_OUTPUT_PROC = "outputProcessor";
    public static final String TAG_PRE_HOOK = "preHook";
    public static final String TAG_POST_HOOK = "postHook";
    protected String pluginName;
    protected String configurationClass;
    protected String preHook;
    protected String postHook;
    protected HashMap<String, String> properties;

    public static void loadConnectorPluginFromConf(final Configuration configuration) throws ParserConfigurationException, SAXException, IOException {
        final String filePath = ConnectorConfiguration.getPluginConf(configuration);
        if (filePath == null || filePath.trim().equals("")) {
            return;
        }
        loadConnectorPlugins(configuration, filePath);
    }

    public static void loadConnectorPluginsAsResource(final String fileName) throws ParserConfigurationException, SAXException, IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = ConnectorPlugin.class.getClassLoader();
        }
        final URL url = classLoader.getResource(fileName);
        if (url == null) {
            throw new ConnectorException(15019, new Object[]{fileName});
        }
        ConnectorPlugin.logger.info((Object) ("load plugins in " + url.toString()));
        loadConnectorPlugins(null, url);
    }

    public static void loadConnectorPlugins(final Configuration configuration, final Object name) throws ParserConfigurationException, SAXException, IOException {
        final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
        Document doc = null;
        Element root = null;
        if (name instanceof URL) {
            doc = builder.parse(((URL) name).toString());
        } else {
            if (!(name instanceof String)) {
                throw new ConnectorException(15025, new Object[]{name.getClass().getName()});
            }
            final Path filePath = new Path((String) name);
            final FileSystem fs = filePath.getFileSystem(configuration);
            ConnectorPlugin.logger.info((Object) ("Using plugin configuration file: " + fs.makeQualified(filePath)));
            if (!fs.exists(filePath)) {
                throw new ConnectorException(15023, new Object[]{filePath});
            }
            if (!fs.isFile(filePath)) {
                throw new ConnectorException(15024, new Object[]{filePath});
            }
            final InputStream is = new BufferedInputStream((InputStream) fs.open(filePath));
            doc = builder.parse(is);
        }
        root = doc.getDocumentElement();
        if (!root.getNodeName().equals("plugins")) {
            throw new ConnectorException(15017, new Object[]{"plugins"});
        }
        final NodeList subroot = root.getChildNodes();
        for (int i = 0; i < subroot.getLength(); ++i) {
            final Node sub = subroot.item(i);
            if (sub instanceof Element) {
                final Element subEle = (Element) sub;
                if (subEle.getTagName().equals("source")) {
                    final ConnectorPlugin plugin = new ConnectorSourcePlugin();
                    plugin.readFields(subEle);
                    plugin.validate();
                    if (ConnectorPlugin.sourcePlugins.get(plugin.getName()) != null) {
                        throw new ConnectorException(15016, new Object[]{plugin.getName()});
                    }
                    ConnectorPlugin.sourcePlugins.put(plugin.getName(), plugin);
                } else {
                    if (!subEle.getTagName().equals("target")) {
                        throw new ConnectorException(15014, new Object[]{subEle.getTagName()});
                    }
                    final ConnectorPlugin plugin = new ConnectorTargetPlugin();
                    plugin.readFields(subEle);
                    plugin.validate();
                    if (ConnectorPlugin.targetPlugins.get(plugin.getName()) != null) {
                        throw new ConnectorException(15016, new Object[]{plugin.getName()});
                    }
                    ConnectorPlugin.targetPlugins.put(plugin.getName(), plugin);
                }
            }
        }
    }

    public static ConnectorPlugin getConnectorSourcePlugin(final String name) throws ConnectorException {
        final ConnectorPlugin plugin = ConnectorPlugin.sourcePlugins.get(name);
        if (plugin == null) {
            throw new ConnectorException(15022, new Object[]{name});
        }
        return plugin;
    }

    public static ConnectorPlugin getConnectorTargetPlugin(final String name) throws ConnectorException {
        final ConnectorPlugin plugin = ConnectorPlugin.targetPlugins.get(name);
        if (plugin == null) {
            throw new ConnectorException(15022, new Object[]{name});
        }
        return plugin;
    }

    public ConnectorPlugin() {
        this.properties = new HashMap<String, String>();
        final String s = "";
        this.postHook = s;
        this.preHook = s;
        this.configurationClass = s;
        this.pluginName = s;
    }

    public String getName() {
        return this.pluginName;
    }

    public void setName(final String name) {
        this.pluginName = name;
    }

    public String getConfigurationClass() {
        return this.configurationClass;
    }

    public void setConfigurationClass(final String configurationClass) {
        this.configurationClass = configurationClass;
    }

    public HashMap<String, String> getProperties() {
        return this.properties;
    }

    public void setProperties(final HashMap<String, String> properties) {
        this.properties = properties;
    }

    public String getPreHook() {
        return this.preHook;
    }

    public void setPreHook(final String preHook) {
        this.preHook = preHook;
    }

    public String getPostHook() {
        return this.postHook;
    }

    public void setPostHook(final String postHook) {
        this.postHook = postHook;
    }

    protected void loadProperties(final Element e) throws ConnectorException {
        if (!e.hasChildNodes()) {
            return;
        }
        final NodeList items = e.getChildNodes();
        for (int i = 0; i < items.getLength(); ++i) {
            final Node item = items.item(i);
            if (item instanceof Element) {
                final Element le = (Element) item;
                if (!le.getTagName().equals("property")) {
                    throw new ConnectorException(15014, new Object[]{le.getTagName()});
                }
                if (le.hasChildNodes()) {
                    final NodeList pros = le.getChildNodes();
                    String name = null;
                    String value = null;
                    for (int k = 0; k < pros.getLength(); ++k) {
                        final Node pro = pros.item(k);
                        if (pro instanceof Element) {
                            final Element field = (Element) pro;
                            if (field.getTagName().equals("name")) {
                                if (field.hasChildNodes()) {
                                    name = ((Text) field.getFirstChild()).getData().trim();
                                }
                            } else if (field.getTagName().equals("value")) {
                                if (field.hasChildNodes()) {
                                    value = ((Text) field.getFirstChild()).getData().trim();
                                }
                            } else if (!field.getTagName().equals("description")) {
                                throw new ConnectorException(15014, new Object[]{field.getTagName()});
                            }
                        }
                    }
                    if (value != null && name != null) {
                        this.properties.put(name, value);
                    }
                }
            }
        }
    }

    protected void validate() throws ConnectorException {
        if (this.pluginName == null) {
            throw new ConnectorException(15015, new Object[]{"name"});
        }
        if (this.configurationClass == null) {
            throw new ConnectorException(15021, new Object[]{this.pluginName});
        }
    }

    public void loadConfiguration(final Configuration conf) {
        for (final Map.Entry<String, String> en : this.properties.entrySet()) {
            final String name = en.getKey();
            final String value = en.getValue();
            if (conf.get(name, (String) null) == null) {
                conf.set(name, value);
            }
        }
    }

    protected abstract void readFields(final Element p0) throws ConnectorException;

    static {
        ConnectorPlugin.logger = LogFactory.getLog((Class) ConnectorPlugin.class);
        try {
            ConnectorPlugin.sourcePlugins = new HashMap<String, ConnectorPlugin>();
            ConnectorPlugin.targetPlugins = new HashMap<String, ConnectorPlugin>();
            loadConnectorPluginsAsResource("teradata.connector.plugins.xml");
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static class ConnectorSourcePlugin extends ConnectorPlugin {
        private String inputFormatClass;
        private String serDeClass;
        private String inputProcessor;

        public ConnectorSourcePlugin() {
            final String inputFormatClass = "";
            this.inputProcessor = inputFormatClass;
            this.serDeClass = inputFormatClass;
            this.inputFormatClass = inputFormatClass;
        }

        public String getInputFormatClass() {
            return this.inputFormatClass;
        }

        public void setInputFormatClass(final String inputFormatClass) {
            this.inputFormatClass = inputFormatClass;
        }

        public String getSerDeClass() {
            return this.serDeClass;
        }

        public void setSerDeClass(final String serDeClass) {
            this.serDeClass = serDeClass;
        }

        public String getInputProcessor() {
            return this.inputProcessor;
        }

        public void setInputProcessor(final String inputProcessor) {
            this.inputProcessor = inputProcessor;
        }

        @Override
        public void readFields(final Element e) throws ConnectorException {
            if (!e.hasChildNodes()) {
                throw new ConnectorException(15020, new Object[]{e.getTagName()});
            }
            final NodeList items = e.getChildNodes();
            for (int j = 0; j < items.getLength(); ++j) {
                final Node item = items.item(j);
                if (item instanceof Element) {
                    final Element el = (Element) item;
                    final String tagName = el.getTagName();
                    if (!el.hasChildNodes()) {
                        throw new ConnectorException(15015, new Object[]{tagName});
                    }
                    if (tagName.equals("name")) {
                        if (el.hasChildNodes()) {
                            this.pluginName = ((Text) el.getFirstChild()).getData();
                        }
                    } else if (!tagName.equals("description")) {
                        if (tagName.equals("configurationClass")) {
                            if (el.hasChildNodes()) {
                                this.configurationClass = ((Text) el.getFirstChild()).getData();
                            }
                        } else if (tagName.equals("inputformatClass")) {
                            if (el.hasChildNodes()) {
                                this.inputFormatClass = ((Text) el.getFirstChild()).getData();
                            }
                        } else if (tagName.equals("serdeClass")) {
                            if (el.hasChildNodes()) {
                                this.serDeClass = ((Text) el.getFirstChild()).getData();
                            }
                        } else if (tagName.equals("inputProcessor")) {
                            if (el.hasChildNodes()) {
                                this.inputProcessor = ((Text) el.getFirstChild()).getData();
                            }
                        } else if (tagName.equals("preHook")) {
                            if (el.hasChildNodes()) {
                                this.preHook = ((Text) el.getFirstChild()).getData();
                            }
                        } else if (tagName.equals("postHook")) {
                            if (el.hasChildNodes()) {
                                this.postHook = ((Text) el.getFirstChild()).getData();
                            }
                        } else {
                            if (!tagName.equals("properties")) {
                                throw new ConnectorException(15014, new Object[]{tagName});
                            }
                            if (el.hasChildNodes()) {
                                this.loadProperties(el);
                            }
                        }
                    }
                }
            }
        }

        @Override
        protected void validate() throws ConnectorException {
            super.validate();
            if (this.inputFormatClass == null || this.serDeClass == null) {
                throw new ConnectorException(15021, new Object[]{this.pluginName});
            }
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("pluginName: " + this.pluginName + "\n");
            sb.append("configurationClass: " + this.configurationClass + "\n");
            sb.append("preHook: " + this.preHook + "\n");
            sb.append("postHook: " + this.postHook + "\n");
            sb.append("inputFormatClass: " + this.inputFormatClass + "\n");
            sb.append("serdeClass: " + this.serDeClass + "\n");
            sb.append("inputProcessor: " + this.inputProcessor + "\n");
            sb.append("properties: " + this.properties.toString() + "\n");
            return sb.toString();
        }
    }

    public static class ConnectorTargetPlugin extends ConnectorPlugin {
        private String outputFormatClass;
        private String serdeClass;
        private String outputProcessor;
        private String converterClass;

        public ConnectorTargetPlugin() {
            final String s = "";
            this.converterClass = s;
            this.outputProcessor = s;
            this.serdeClass = s;
            this.outputFormatClass = s;
        }

        public String getOutputFormatClass() {
            return this.outputFormatClass;
        }

        public void setOutputFormatClass(final String outputformatClass) {
            this.outputFormatClass = outputformatClass;
        }

        public String getSerdeClass() {
            return this.serdeClass;
        }

        public void setSerdeClass(final String serdeClass) {
            this.serdeClass = serdeClass;
        }

        public String getOutputProcessor() {
            return this.outputProcessor;
        }

        public void setOutputProcessor(final String outputProcessor) {
            this.outputProcessor = outputProcessor;
        }

        public String getConverterClass() {
            return this.converterClass;
        }

        public void setConverterClass(final String converterClass) {
            this.converterClass = converterClass;
        }

        @Override
        public void readFields(final Element e) throws ConnectorException {
            if (!e.hasChildNodes()) {
                throw new ConnectorException(15020, new Object[]{e.getTagName()});
            }
            final NodeList items = e.getChildNodes();
            for (int j = 0; j < items.getLength(); ++j) {
                final Node item = items.item(j);
                if (item instanceof Element) {
                    final Element el = (Element) item;
                    final String tagName = el.getTagName();
                    if (!el.hasChildNodes()) {
                        throw new ConnectorException(15015, new Object[]{tagName});
                    }
                    if (tagName.equals("name")) {
                        if (el.hasChildNodes()) {
                            this.pluginName = ((Text) el.getFirstChild()).getData();
                        }
                    } else if (!tagName.equals("description")) {
                        if (tagName.equals("configurationClass")) {
                            if (el.hasChildNodes()) {
                                this.configurationClass = ((Text) el.getFirstChild()).getData();
                            }
                        } else if (tagName.equals("outputformatClass")) {
                            if (el.hasChildNodes()) {
                                this.outputFormatClass = ((Text) el.getFirstChild()).getData();
                            }
                        } else if (tagName.equals("serdeClass")) {
                            if (el.hasChildNodes()) {
                                this.serdeClass = ((Text) el.getFirstChild()).getData();
                            }
                        } else if (tagName.equals("converterClass")) {
                            if (el.hasChildNodes()) {
                                this.converterClass = ((Text) el.getFirstChild()).getData();
                            }
                        } else if (tagName.equals("outputProcessor")) {
                            if (el.hasChildNodes()) {
                                this.outputProcessor = ((Text) el.getFirstChild()).getData();
                            }
                        } else if (tagName.equals("preHook")) {
                            if (el.hasChildNodes()) {
                                this.preHook = ((Text) el.getFirstChild()).getData();
                            }
                        } else if (tagName.equals("postHook")) {
                            if (el.hasChildNodes()) {
                                this.postHook = ((Text) el.getFirstChild()).getData();
                            }
                        } else {
                            if (!tagName.equals("properties")) {
                                throw new ConnectorException(15014, new Object[]{tagName});
                            }
                            if (el.hasChildNodes()) {
                                this.loadProperties(el);
                            }
                        }
                    }
                }
            }
        }

        @Override
        protected void validate() throws ConnectorException {
            super.validate();
            if (this.outputFormatClass == null || this.serdeClass == null) {
                throw new ConnectorException(15021, new Object[]{this.pluginName});
            }
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("name: " + this.pluginName + "\n");
            sb.append("configurationClass: " + this.configurationClass + "\n");
            sb.append("preHook: " + this.preHook + "\n");
            sb.append("postHook: " + this.postHook + "\n");
            sb.append("outputFormatClass: " + this.outputFormatClass + "\n");
            sb.append("serdeClass: " + this.serdeClass + "\n");
            sb.append("outputProcessor: " + this.outputProcessor + "\n");
            sb.append("converterClass: " + this.converterClass + "\n");
            sb.append("properties: " + this.properties + "\n");
            return sb.toString();
        }
    }
}
