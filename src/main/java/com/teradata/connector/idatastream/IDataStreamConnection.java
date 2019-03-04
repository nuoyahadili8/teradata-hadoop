package com.teradata.connector.idatastream;

import com.teradata.connector.common.exception.ConnectorException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * @author Administrator
 */
public class IDataStreamConnection {
    private boolean connectionOpen;
    private boolean leserver;
    private int port;
    private String host;
    private Socket socket;
    private BufferedOutputStream outputstream;
    private BufferedInputStream inputstream;

    public IDataStreamConnection(final String host, final int port) {
        this.connectionOpen = false;
        this.leserver = true;
        this.port = -1;
        this.host = null;
        this.socket = null;
        this.outputstream = null;
        this.inputstream = null;
        this.host = host;
        this.port = port;
    }

    public void connect() throws ConnectorException {
        if (this.connectionOpen) {
            return;
        }
        try {
            (this.socket = new Socket(this.host, this.port)).setSoTimeout(0);
            this.outputstream = new BufferedOutputStream(this.socket.getOutputStream());
            this.inputstream = new BufferedInputStream(this.socket.getInputStream());
            this.connectionOpen = true;
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
    }

    public void disconnect() throws ConnectorException {
        if (!this.connectionOpen) {
            return;
        }
        try {
            this.outputstream.close();
            this.inputstream.close();
            this.socket.close();
            this.connectionOpen = false;
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
    }

    public OutputStream getOutputStream() {
        return this.outputstream;
    }

    public InputStream getInputStream() {
        return this.inputstream;
    }

    public boolean isClosed() {
        return !this.connectionOpen;
    }

    public void setLEServer(final boolean leserver) {
        this.leserver = leserver;
    }

    public boolean isLEServer() {
        return this.leserver;
    }
}
