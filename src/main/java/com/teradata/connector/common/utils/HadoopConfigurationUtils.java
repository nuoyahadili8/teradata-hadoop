package com.teradata.connector.common.utils;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.net.NetUtils;

/**
 * @author Administrator
 */
public class HadoopConfigurationUtils {
    private static Log logger= LogFactory.getLog((Class) HadoopConfigurationUtils.class);;
    private static final int SELECT_HOST_RANGE_MIN = 10;
    private static final int MIN_PORT_NUMBER = 8678;
    private static final int MAX_PORT_NUMBER = 65535;
    private static final int REACHABLE_TIMEOUT = 2000;
    private static final PathFilter FILTER = new PathFilter() {
        @Override
        public boolean accept(Path p) {
            String name = p.getName();
            return (name.startsWith("_") || name.startsWith(".")) ? false : true;
        }
    };

    public static int getMaxMapTasks(final JobContext context) throws ConnectorException {
        try {
            final JobConf jobConf = new JobConf(context.getConfiguration());
            final JobClient jc = new JobClient(jobConf);
            final ClusterStatus status = jc.getClusterStatus(true);
            return status.getMaxMapTasks();
        } catch (IOException e) {
            throw new ConnectorException(e.getMessage(), e);
        } catch (UnsupportedOperationException e2) {
            HadoopConfigurationUtils.logger.warn((Object) "Get Cluster Status function is not supported on this platform");
            return -1;
        }
    }

    public static int getMaxReduceTasks(final JobContext context) throws ConnectorException {
        try {
            final JobConf jobConf = new JobConf(context.getConfiguration());
            final JobClient jc = new JobClient(jobConf);
            final ClusterStatus status = jc.getClusterStatus(true);
            return status.getMaxReduceTasks();
        } catch (IOException e) {
            throw new ConnectorException(e.getMessage(), e);
        } catch (UnsupportedOperationException e2) {
            HadoopConfigurationUtils.logger.warn((Object) "Get Cluster Status function is not supported on this platform");
            return -1;
        }
    }

    public static void utilizeMaxConcurrentMappers(final JobContext context) throws ConnectorException {
        int numDataNodes = 0;
        try {
            final Class<?> ClusterClass = Class.forName("org.apache.hadoop.mapreduce.Cluster");
            final Constructor<?> ClusterConstructor = ClusterClass.getConstructor(Configuration.class);
            final Method ClusterGetClusterStatusMethod = ClusterClass.getMethod("getClusterStatus", (Class<?>[]) new Class[0]);
            final Object ClusterInstance = ClusterConstructor.newInstance(context.getConfiguration());
            final ClusterMetrics metrics = (ClusterMetrics) ClusterGetClusterStatusMethod.invoke(ClusterInstance, new Object[0]);
            numDataNodes = metrics.getTaskTrackerCount();
            HadoopConfigurationUtils.logger.debug((Object) ("ClusterMetrics returned " + numDataNodes + " data nodes in the cluster"));
        } catch (Exception e) {
            e.printStackTrace();
            HadoopConfigurationUtils.logger.warn((Object) "Num mappers throttle functionality requires APIs which are not available on the cluster, mapper throttle functionality is being bypassed");
            return;
        }
        final Configuration config = context.getConfiguration();
        final int maxYarnMemory = config.getInt("yarn.scheduler.maximum-allocation-mb", 0);
        final int mapMemory = config.getInt("mapreduce.map.memory.mb", 0);
        if (maxYarnMemory <= 0 || mapMemory <= 0) {
            HadoopConfigurationUtils.logger.warn((Object) "Num mappers throttle functionality requires YARN which is not available on the cluster, mapper throttle functionality is being bypassed");
            return;
        }
        int maxContainers = (int) Math.floor(maxYarnMemory / (double) mapMemory);
        maxContainers *= numDataNodes;
        HadoopConfigurationUtils.logger.debug((Object) ("Yarn is configured to allocate " + maxYarnMemory + " mb per datanode"));
        HadoopConfigurationUtils.logger.debug((Object) ("MapReduce is configured to allocate " + mapMemory + " mb per mapper"));
        HadoopConfigurationUtils.logger.debug((Object) ("The cluster can handle " + maxContainers + " containers concurrently"));
        String queuename = context.getConfiguration().get("mapred.job.queue.name", "");
        queuename = (queuename.isEmpty() ? context.getConfiguration().get("mapreduce.job.queuename") : queuename);
        queuename = (queuename.isEmpty() ? "default" : queuename);
        final int minMappers = ConnectorConfiguration.getThrottleNumMappersMinMappers(context.getConfiguration());
        final int retryTime = ConnectorConfiguration.getThrottleNumMappersRetrySeconds(context.getConfiguration());
        final int originalRetryCount;
        int retryCount = originalRetryCount = ConnectorConfiguration.getThrottleNumMappersRetryCount(context.getConfiguration());
        if (minMappers != 0 && (retryTime <= 0 || retryCount <= 0)) {
            throw new ConnectorException(15050);
        }
        int usedContainers = 0;
        int maxTDCHContainers = 0;
        float capacity = 0.0f;
        float maxQueueContainers = 0.0f;
        try {
            final Class<?> YarnClientClass = Class.forName("org.apache.hadoop.yarn.client.api.YarnClient");
            final Class<?> QueueInfoClass = Class.forName("org.apache.hadoop.yarn.api.records.QueueInfo");
            final Class<?> ApplicationReportClass = Class.forName("org.apache.hadoop.yarn.api.records.ApplicationReport");
            final Class<?> AppUsageReportClass = Class.forName("org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport");
            final Method YarnClientCreateYarnClientMethod = YarnClientClass.getMethod("createYarnClient", (Class<?>[]) new Class[0]);
            final Method YarnClientInitMethod = YarnClientClass.getMethod("init", Configuration.class);
            final Method YarnClientStartMethod = YarnClientClass.getMethod("start", (Class<?>[]) new Class[0]);
            final Method YarnClientGetQueueInfoMethod = YarnClientClass.getMethod("getQueueInfo", String.class);
            final Method QueueInfoGetCapacityMethod = QueueInfoClass.getMethod("getCapacity", (Class<?>[]) new Class[0]);
            final Method QueueInfoGetApplicationsMethod = QueueInfoClass.getMethod("getApplications", (Class<?>[]) new Class[0]);
            final Method ApplicationReportGetAppUsageReportMethod = ApplicationReportClass.getMethod("getApplicationResourceUsageReport", (Class<?>[]) new Class[0]);
            final Method AppUsageReportGetReservedContainersMethod = AppUsageReportClass.getMethod("getNumReservedContainers", (Class<?>[]) new Class[0]);
            final Method AppUsageReportGetUsedContainersMethod = AppUsageReportClass.getMethod("getNumUsedContainers", (Class<?>[]) new Class[0]);
            final Object YarnClientInstance = YarnClientCreateYarnClientMethod.invoke(null, new Object[0]);
            YarnClientInitMethod.invoke(YarnClientInstance, context.getConfiguration());
            YarnClientStartMethod.invoke(YarnClientInstance, new Object[0]);
            Object QueueInfoInstance = YarnClientGetQueueInfoMethod.invoke(YarnClientInstance, queuename);
            capacity = (float) QueueInfoGetCapacityMethod.invoke(QueueInfoInstance, new Object[0]);
            maxQueueContainers = (float) Math.ceil(maxContainers * capacity);
            HadoopConfigurationUtils.logger.debug((Object) ("Queue " + queuename + " has a static capacity of " + capacity + ", equivalent to " + maxQueueContainers + " containers"));
            for (final Object ApplicationReportInstance : (List) QueueInfoGetApplicationsMethod.invoke(QueueInfoInstance, new Object[0])) {
                final Object AppUsageReportInstance = ApplicationReportGetAppUsageReportMethod.invoke(ApplicationReportInstance, new Object[0]);
                usedContainers += (int) AppUsageReportGetReservedContainersMethod.invoke(AppUsageReportInstance, new Object[0]);
                usedContainers += (int) AppUsageReportGetUsedContainersMethod.invoke(AppUsageReportInstance, new Object[0]);
            }
            HadoopConfigurationUtils.logger.debug((Object) ("Queue " + queuename + " has applications running which are utilizing " + usedContainers + " containers"));
            maxTDCHContainers = (int) (maxQueueContainers - usedContainers);
            HadoopConfigurationUtils.logger.debug((Object) "Max Concurrent Containers for TDCH:");
            HadoopConfigurationUtils.logger.debug((Object) ("\tMax containers for the cluster:\t" + maxContainers));
            HadoopConfigurationUtils.logger.debug((Object) ("\tMax containers for the queue:\t" + maxQueueContainers));
            HadoopConfigurationUtils.logger.debug((Object) ("\tMax concurrent containers for TDCH:\t" + maxTDCHContainers));
            HadoopConfigurationUtils.logger.debug((Object) ("\tMin containers requested by user:\t" + minMappers));
            if (minMappers != 0 && maxTDCHContainers < minMappers && retryCount != 0) {
                HadoopConfigurationUtils.logger.info((Object) ("Queue " + queuename + " can run less than " + minMappers + " containers concurrently, job will be submitted once more than " + minMappers + " containers become available"));
            }
            while (minMappers != 0 && maxTDCHContainers < minMappers && retryCount-- != 0) {
                Thread.sleep(retryTime * 1000);
                usedContainers = 0;
                QueueInfoInstance = YarnClientGetQueueInfoMethod.invoke(YarnClientInstance, queuename);
                for (final Object ApplicationReportInstance : (List) QueueInfoGetApplicationsMethod.invoke(QueueInfoInstance, new Object[0])) {
                    final Object AppUsageReportInstance = ApplicationReportGetAppUsageReportMethod.invoke(ApplicationReportInstance, new Object[0]);
                    usedContainers += (int) AppUsageReportGetReservedContainersMethod.invoke(AppUsageReportInstance, new Object[0]);
                    usedContainers += (int) AppUsageReportGetUsedContainersMethod.invoke(AppUsageReportInstance, new Object[0]);
                }
                HadoopConfigurationUtils.logger.debug((Object) ("Queue " + queuename + " has applications running which are utilizing " + usedContainers + " containers"));
                maxTDCHContainers = (int) (maxQueueContainers - usedContainers);
                HadoopConfigurationUtils.logger.debug((Object) "Max Concurrent Containers for TDCH:");
                HadoopConfigurationUtils.logger.debug((Object) ("\tMax containers for the cluster:\t" + maxContainers));
                HadoopConfigurationUtils.logger.debug((Object) ("\tMax containers for the queue:\t" + maxQueueContainers));
                HadoopConfigurationUtils.logger.debug((Object) ("\tMax concurrent containers for TDCH:\t" + maxTDCHContainers));
                HadoopConfigurationUtils.logger.debug((Object) ("\tMin containers requested by user:\t" + minMappers));
            }
        } catch (Exception e2) {
            e2.printStackTrace();
            HadoopConfigurationUtils.logger.warn((Object) "Num mappers throttle functionality requires YARN which is not available on the cluster, mapper throttle functionality is being bypassed");
            return;
        }
        final int nummappers = ConnectorConfiguration.getNumMappers(config);
        if (maxTDCHContainers <= 0) {
            throw new ConnectorException(15048, new Object[]{queuename});
        }
        if (minMappers != 0 && maxTDCHContainers < minMappers) {
            throw new ConnectorException(15049, new Object[]{minMappers, queuename, originalRetryCount, retryTime});
        }
        if (nummappers > maxTDCHContainers) {
            HadoopConfigurationUtils.logger.warn((Object) ("User-defined nummappers value utilizes more containers (" + nummappers + ") than the cluster can handle concurrently (" + maxTDCHContainers + "), overwiting user-defined value"));
            ConnectorConfiguration.setNumMappers(config, maxTDCHContainers);
        }
    }

    public static String[] getAllActiveHosts(final JobContext context) throws ConnectorException {
        String[] hosts = new String[0];
        try {
            final JobConf jobConf = new JobConf(context.getConfiguration());
            final JobClient jc = new JobClient(jobConf);
            final ClusterStatus status = jc.getClusterStatus(true);
            final Collection<String> trackers = (Collection<String>) status.getActiveTrackerNames();
            if (trackers.size() > 0) {
                hosts = new String[trackers.size()];
                int count = 0;
                for (final String tracker : trackers) {
                    final int colonpos = tracker.indexOf(58);
                    String host = (colonpos >= 0) ? tracker.substring(0, colonpos) : tracker;
                    final int underscorepos = host.indexOf(95);
                    if (underscorepos >= 0) {
                        host = host.substring(underscorepos + 1);
                    }
                    hosts[count++] = host;
                }
            }
        } catch (IOException e) {
            throw new ConnectorException(e.getMessage(), e);
        } catch (UnsupportedOperationException e2) {
            HadoopConfigurationUtils.logger.warn((Object) "getClusterStatus is not supported on this platform");
        }
        return hosts;
    }

    public static int getUnusedPort() throws ConnectorException {
        int i = MIN_PORT_NUMBER;
        while (i < MAX_PORT_NUMBER) {
            try {
                new ServerSocket(i).close();
                return i;
            } catch (IOException e) {
                i++;
            }
        }
        throw new ConnectorException((int) ErrorCode.FASTLOAD_NO_AVAILABLE_PORT);
    }

    public static ServerSocket createServerSocket(int port, int backlog) throws ConnectorException {
        if (port == 0) {
            int i = MIN_PORT_NUMBER;
            while (i < MAX_PORT_NUMBER) {
                try {
                    return new ServerSocket(i, backlog);
                } catch (IOException e) {
                    i++;
                }
            }
            throw new ConnectorException((int) ErrorCode.FASTLOAD_NO_AVAILABLE_PORT);
        }
        try {
            return new ServerSocket(port, backlog);
        } catch (IOException e2) {
            throw new ConnectorException(e2.getMessage(), e2);
        }
    }

    public static String getClusterNodeInterface(final JobContext context) throws ConnectorException {
        final Configuration configuration = context.getConfiguration();
        final String hdfsInterfaceName = configuration.get("dfs.datanode.dns.interface", "default");
        final String mapreduceInterfaceName = configuration.get("mapred.tasktracker.dns.interface", "default");
        try {
            if (!hdfsInterfaceName.equals("default") || !mapreduceInterfaceName.equals("default")) {
                String interfaceName = null;
                if (!hdfsInterfaceName.equals("default")) {
                    interfaceName = hdfsInterfaceName;
                } else {
                    interfaceName = mapreduceInterfaceName;
                }
                final NetworkInterface networkInterface = NetworkInterface.getByName(interfaceName);
                final Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    final InetAddress inetAddress = inetAddresses.nextElement();
                    if (inetAddress instanceof Inet4Address) {
                        return inetAddress.getHostAddress();
                    }
                }
                return InetAddress.getLocalHost().getHostName();
            }
            final Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress trackerAddr = null;
            if (configuration.get("mapred.job.tracker", "local").equals("local")) {
                trackerAddr = InetAddress.getLocalHost();
            } else {
                final String jobtracker = configuration.get("mapred.job.tracker", "localhost:8012");
                try {
                    trackerAddr = NetUtils.createSocketAddr(jobtracker).getAddress();
                } catch (Exception e2) {
                    final String[] allActiveHosts = getAllActiveHosts(context);
                    final int length = allActiveHosts.length;
                    int i = 0;
                    while (i < length) {
                        final String host = allActiveHosts[i];
                        try {
                            trackerAddr = NetUtils.createSocketAddr(host, 7).getAddress();
                        } catch (Exception ex) {
                            ++i;
                        }
                    }
                }
            }
            if (trackerAddr == null) {
                throw new ConnectorException(22015);
            }
            boolean isReachableSuccess = false;
            while (netInterfaces.hasMoreElements()) {
                final NetworkInterface networkInterface2 = netInterfaces.nextElement();
                if (trackerAddr.isReachable(networkInterface2, 0, 2000)) {
                    isReachableSuccess = true;
                    final Enumeration<InetAddress> inetAddresses2 = networkInterface2.getInetAddresses();
                    while (inetAddresses2.hasMoreElements()) {
                        final InetAddress inetAddress2 = inetAddresses2.nextElement();
                        if (inetAddress2 instanceof Inet4Address) {
                            return inetAddress2.getHostAddress();
                        }
                    }
                }
            }
            if (isReachableSuccess) {
                throw new ConnectorException(22008);
            }
            throw new ConnectorException(22014);
        } catch (IOException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }

    public static String[] selectUniqueActiveHosts(final String[] allActiveHosts, final int numberOfHosts) {
        if (allActiveHosts == null) {
            return new String[0];
        }
        if (allActiveHosts.length <= numberOfHosts) {
            return allActiveHosts;
        }
        final String[] hosts = new String[numberOfHosts];
        final int range = allActiveHosts.length / numberOfHosts;
        if (range >= 10) {
            final Random rand = new Random();
            for (int i = 0; i < numberOfHosts; ++i) {
                hosts[i] = allActiveHosts[rand.nextInt(range) + range * i];
            }
        } else {
            final ArrayList<String> allActiveHostArrays = new ArrayList<String>(Arrays.asList(allActiveHosts));
            Collections.shuffle(allActiveHostArrays);
            for (int i = 0; i < numberOfHosts; ++i) {
                hosts[i] = allActiveHostArrays.get(i);
            }
        }
        return hosts;
    }

    public static String getAllFilePaths(final Configuration configuration, final String[] paths) throws IOException {
        final List<FileStatus> result = new ArrayList<FileStatus>();
        for (final String multiPaths : paths) {
            final String[] split;
            final String[] splitPaths = split = multiPaths.split(",");
            for (String path : split) {
                path = path.trim();
                final Path p = new Path(path);
                final FileSystem fs = p.getFileSystem(configuration);
                addInputPathRecursively(result, fs, p, HadoopConfigurationUtils.FILTER);
            }
        }
        final int size = result.size();
        if (size <= 0) {
            return "";
        }
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < size; ++i) {
            builder.append(result.get(i).getPath().toString()).append(',');
        }
        return builder.substring(0, builder.length() - 1);
    }

    public static List<Path> getAllFilePaths(final Configuration configuration, final String path) throws IOException {
        final List<FileStatus> result = new ArrayList<FileStatus>();
        final List<Path> resultPaths = new ArrayList<Path>();
        final Path p = new Path(path);
        final FileSystem fs = p.getFileSystem(configuration);
        addInputPathRecursively(result, fs, p, HadoopConfigurationUtils.FILTER);
        for (int size = result.size(), i = 0; i < size; ++i) {
            resultPaths.add(result.get(i).getPath());
        }
        return resultPaths;
    }

    protected static void addInputPathRecursively(final List<FileStatus> result, final FileSystem fs, final Path path, final PathFilter inputFilter) throws IOException {
        for (final FileStatus stat : fs.listStatus(path, inputFilter)) {
            if (stat.isDir()) {
                addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
            } else {
                result.add(stat);
            }
        }
    }

    public static int getNextFilePathIncrement(final Configuration configuration, final String folderPath, String filePrefix, final int numDigit) throws IOException {
        final Path p = new Path(folderPath);
        final FileSystem fs = p.getFileSystem(configuration);
        if (!fs.exists(p) || numDigit <= 0) {
            return 0;
        }
        if (fs.isFile(p)) {
            throw new ConnectorException(15001);
        }
        if (filePrefix == null) {
            filePrefix = "";
        }
        final String fp = p.toUri().toString() + "/";
        String filepath = fp + filePrefix + String.format("%0" + numDigit + "d", 0);
        if (!fs.isFile(new Path(filepath))) {
            return 0;
        }
        int beginDigit = 1;
        int endDigit = 1;
        boolean firstNumberFound = false;
        for (int i = 1; i < numDigit + 1; ++i) {
            beginDigit = endDigit;
            endDigit *= 10;
            filepath = fp + filePrefix + String.format("%0" + numDigit + "d", endDigit);
            if (!fs.isFile(new Path(filepath))) {
                firstNumberFound = true;
                break;
            }
        }
        if (firstNumberFound) {
            while (beginDigit != endDigit) {
                final int middleDigit = (beginDigit + endDigit) / 2;
                filepath = fp + filePrefix + String.format("%0" + numDigit + "d", middleDigit);
                if (fs.isFile(new Path(filepath))) {
                    beginDigit = middleDigit + 1;
                } else {
                    endDigit = middleDigit;
                }
            }
            return endDigit;
        }
        throw new ConnectorException(15002);
    }

    @Deprecated
    public static void configureMapSpeculative(final Configuration configuration, final boolean value) {
        configuration.setBoolean("mapred.map.tasks.speculative.execution", value);
    }

    public static String getOutputBaseName(final JobContext context) {
        return context.getConfiguration().get("mapreduce.output.basename", "part");
    }

    public static String getAliasFileFormatName(final String fileFormat) {
        if (fileFormat.equalsIgnoreCase("orc")) {
            return "orcfile";
        }
        return fileFormat;
    }
}
