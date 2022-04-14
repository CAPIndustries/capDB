package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.PrintWriter;

import java.net.Socket;
import java.net.UnknownHostException;
import java.net.SocketTimeoutException;

import org.apache.log4j.Logger;

import shared.messages.KVMessage;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import exceptions.InvalidMessageException;

import app_kvServer.IKVServer.OperatingState;

public class ReplicaConnection {

    private String address;
    private int port;
    private Socket clientSocket;
    private OutputStream output;
    private InputStream input;
    public int output_port;
    public String name;
    public OperatingState operatingState = OperatingState.NORMAL;
    public boolean added = false;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;
    private static final int RESPONSE_TIME = 90 * 1000;
    private static Logger logger = Logger.getRootLogger();

    public ReplicaConnection(String replicaAddress, int replicaPort, String name) {
        this.address = replicaAddress;
        this.port = replicaPort;
        this.name = name;
    }

    public void connect() throws UnknownHostException, IOException {
        logger.info("Trying to connect to: " + this);

        try {
            clientSocket = new Socket(address, port);
            clientSocket.setSoTimeout(RESPONSE_TIME);
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();
            output_port = clientSocket.getLocalPort();
            // Skip the first metadata response (essentially clearing the buffer):
            receiveMessage(false);
        } catch (Exception e) {
            exceptionLogger(e);
        }

        logger.info("Connected to " + this.address + ":" + this.port);

    }

    public IKVMessage put(String key, String value) throws Exception {
        KVMessage msg = new KVMessage(key, value, StatusType.PUT);
        sendMessage(msg, false);

        KVMessage res = receiveMessage(false);
        if (res == null) {
            res = new KVMessage(key, "Timed out after " + RESPONSE_TIME / 1000 + " seconds", StatusType.PUT_ERROR);
        }

        return res;
    }

    public IKVMessage get(String key) throws Exception {
        KVMessage msg = new KVMessage(key, null, StatusType.GET);
        sendMessage(msg, false);

        KVMessage res = receiveMessage(false);
        if (res == null) {
            res = new KVMessage(key, "Timed out after " + RESPONSE_TIME / 1000 + " seconds", StatusType.GET_ERROR);
        }

        return res;
    }

    public synchronized void sendMessage(KVMessage msg, boolean heartbeat) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
    }

    public synchronized KVMessage receiveMessage(boolean heartbeat)
            throws IOException, InvalidMessageException, Exception {
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        if (read == -1) {
            throw new Exception("Reached end of stream!");
        }

        while (read != LINE_FEED && read != -1 && reading) {/* LF, error, drop */
            /* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                if (msgBytes == null) {
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            bufferBytes[index] = read;
            index++;

            /* stop reading if DROP_SIZE is reached */
            if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if (msgBytes == null) {
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;

        /* build final result */
        KVMessage msg = new KVMessage(msgBytes);

        return msg;
    }

    private static void exceptionLogger(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        logger.error(sw.toString());
    }

    public void shutDown() {
        try {
            if (clientSocket != null) {
                logger.info("Trying to disconnect ...");
                input.close();
                output.close();
                clientSocket.close();
                clientSocket = null;
                logger.info("Connection closed!");
            }
        } catch (IOException ioe) {
            logger.error("Unable to close connection!", ioe);
        }
    }
}