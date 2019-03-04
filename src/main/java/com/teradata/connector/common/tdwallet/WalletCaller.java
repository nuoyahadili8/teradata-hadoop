package com.teradata.connector.common.tdwallet;

import org.apache.hadoop.mapreduce.*;
import com.teradata.connector.common.exception.*;
import org.apache.tools.ant.util.*;

import java.rmi.dgc.*;

import com.teradata.connector.common.utils.*;

import java.math.*;

import org.apache.commons.io.*;

import java.util.*;
import java.net.*;
import java.io.*;

/**
 * @author Administrator
 */
public class WalletCaller {
    private static String myTempDirPathWithSeparator;
    private static final Process process;

    public static final void performSubstitutions(final JobContext context) {
        performSubstitutions(context, WalletCaller.myTempDirPathWithSeparator);
    }

    private static final native void performSubstitutions(final JobContext p0, final String p1);

    private static final Process getCleanupProcess() throws ExceptionInInitializerError {
        try {
            final String osNameJava = System.getProperty("os.name");
            if (null == osNameJava) {
                throw new ConnectorException(15038);
            }
            final Map<String, String> m = new LinkedHashMap<String, String>();
            m.put("Linux", "suselinux");
            m.put("Windows", "nt");
            m.put("Mac", "macos");
            m.put("SunOS", "solaris");
            m.put("Solaris", "solaris");
            m.put("HP-UX", "hpux");
            m.put("AIX", "aix");
            final Map<String, String> javaOtbeOsNameMap = Collections.unmodifiableMap((Map<? extends String, ? extends String>) m);
            for (final Map.Entry<String, String> entry : javaOtbeOsNameMap.entrySet()) {
                if (osNameJava.contains(entry.getKey())) {
                    final String osNameOtbe = entry.getValue();
                    final String archNameJava = System.getProperty("os.arch");
                    if (null == archNameJava) {
                        throw new ConnectorException(15039);
                    }
                    final Map<String, String> i = new LinkedHashMap<String, String>();
                    i.put("x86", "i386");
                    i.put("i386", "i386");
                    i.put("i686", "i386");
                    i.put("x86_64", "x8664");
                    i.put("amd64", "x8664");
                    i.put("IA64N", "ia64");
                    i.put("RISC", "pa.32");
                    i.put("Power", "power.32");
                    i.put("ppc", "power.32");
                    i.put("sparc", "sparc.32");
                    final Map<String, String> javaOtbeArchNameMap = Collections.unmodifiableMap((Map<? extends String, ? extends String>) i);
                    for (final Map.Entry<String, String> entry2 : javaOtbeArchNameMap.entrySet()) {
                        if (archNameJava.contains(entry2.getKey())) {
                            final String chipNameOtbe = entry2.getValue();
                            final String libraryName = "WalletCaller-" + osNameOtbe + "-" + chipNameOtbe;
                            final String walletCallerLibraryName = System.mapLibraryName(libraryName);
                            if (null == walletCallerLibraryName) {
                                throw new ConnectorException(15042, new Object[]{libraryName});
                            }
                            final InputStream walletStream = WalletCaller.class.getResourceAsStream(walletCallerLibraryName);
                            if (null == walletStream) {
                                throw new ConnectorException(15037, new Object[]{walletCallerLibraryName});
                            }
                            final String javaExecutable = JavaEnvUtils.getJreExecutable("java");
                            if (null == javaExecutable) {
                                throw new ConnectorException(15040);
                            }
                            final String fileSeparator = System.getProperty("file.separator");
                            if (null == fileSeparator) {
                                throw new ConnectorException(15044);
                            }
                            final String pathSeparator = System.getProperty("path.separator");
                            if (null == pathSeparator) {
                                throw new ConnectorException(15047);
                            }
                            final Class cleanupClass = Cleanup.class;
                            final String cleanupResourceName = cleanupClass.getName().replace('.', '/') + ".class";
                            final URL url = cleanupClass.getClassLoader().getResource(cleanupResourceName);
                            final String protocol = url.getProtocol();
                            final String path = url.getPath();
                            if (!"file".equals(protocol)) {
                                throw new ConnectorException(15041);
                            }
                            final String dir = path.substring(0, path.length() - cleanupResourceName.length());
                            final String javaClassPath = dir + pathSeparator + dir + "lib" + fileSeparator + "*";
                            final String javaIoTmpdir = System.getProperty("java.io.tmpdir");
                            if (null == javaIoTmpdir) {
                                throw new ConnectorException(15043);
                            }
                            final File tempDir = new File(javaIoTmpdir);
                            tempDir.mkdirs();
                            final ProcessBuilder processBuilder = new ProcessBuilder(new String[]{javaExecutable, "-classpath", javaClassPath, Cleanup.class.getName()}).directory(tempDir);
                            final Process process = processBuilder.start();
                            final ObjectOutputStream objectOutputStream = new ObjectOutputStream(process.getOutputStream());
                            final VMID vmid = new VMID();
                            if (null == vmid) {
                                throw new ConnectorException(15045);
                            }
                            final File myTempDir = new File(tempDir.getAbsolutePath() + fileSeparator + "." + new BigInteger(vmid.toString().getBytes(StandardCharsets.UTF_8)).toString(36) + new Object() {
                            }.getClass().getEnclosingClass().getCanonicalName());
                            final String myTempDirPath = myTempDir.getAbsolutePath();
                            objectOutputStream.writeObject(myTempDirPath);
                            objectOutputStream.flush();
                            if (!myTempDir.mkdirs()) {
                                throw new ConnectorException(15046, new Object[]{myTempDirPath});
                            }
                            WalletCaller.myTempDirPathWithSeparator = myTempDirPath + fileSeparator;
                            final File tempFile = new File(WalletCaller.myTempDirPathWithSeparator + walletCallerLibraryName);
                            final OutputStream outputStream = new FileOutputStream(tempFile);
                            IOUtils.copy(walletStream, outputStream);
                            outputStream.close();
                            outputStream.close();
                            System.load(tempFile.getAbsolutePath());
                            return process;
                        }
                    }
                    throw new ConnectorException(15036, new Object[]{archNameJava});
                }
            }
            throw new ConnectorException(15035, new Object[]{osNameJava});
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    static {
        process = getCleanupProcess();
    }
}
