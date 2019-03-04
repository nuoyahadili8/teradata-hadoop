package com.teradata.connector.common.tdwallet;

import java.io.*;

import org.apache.commons.io.*;
import com.teradata.connector.common.utils.*;
import org.apache.commons.logging.*;

/**
 * @author Administrator
 */
public class Cleanup {
    public static void main(final String[] args) {
        try {
            final File dirname = new File((String) new ObjectInputStream(System.in).readObject());
            try {
                System.in.read();
            } catch (Throwable t2) {
            } finally {
                FileUtils.deleteDirectory(dirname);
            }
        } catch (Throwable t) {
            final Log logger = LogFactory.getLog((Class) Cleanup.class);
            logger.warn((Object) ConnectorStringUtils.getExceptionStack(t));
        }
    }
}
