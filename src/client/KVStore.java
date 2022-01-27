package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import shared.messages.KVMessage;
import shared.messages.TextMessage;
import shared.messages.KVMessage.StatusType;

public class KVStore implements KVCommInterface {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVStore> ";
	private boolean running;
	private String address;
	private int port;
	private Socket clientSocket;
	private OutputStream output;
 	private InputStream input;

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}
	
	@Override
	public void connect() throws UnknownHostException, IOException {
		System.out.println(PROMPT + "Trying to connect ...");
		logger.info("Trying to connect ...");

		clientSocket = new Socket(address, port);
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
		// TODO: Set timeout here
		setRunning(true);

		System.out.println(PROMPT + "Connected!");
		logger.info("Connection established to " + address + " on port " + port);
	}

	@Override
	public void disconnect() {
		try {
			System.out.println(PROMPT + "Trying to disconnect ...");
			logger.info("Trying to disconnect ...");
			setRunning(false);
			if (clientSocket != null) {
				input.close();
				output.close();
				clientSocket.close();
				clientSocket = null;
				System.out.println(PROMPT + "Connection closed!");
				logger.info("Connection closed!");
			}
		} catch (IOException ioe) {
			printError("Unable to close connection!");
			logger.error("Unable to close connection!", ioe);
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		TextMessage msg = new TextMessage(key, value, StatusType.PUT);
		sendMessage(msg);
		
		TextMessage res = receiveMessage();

		return res;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		TextMessage msg = new TextMessage(key, null, StatusType.GET);
		sendMessage(msg);

		TextMessage res = receiveMessage();

		return res;
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean run) {
		running = run;
	}

	public synchronized void closeConnection() {
		disconnect();
	}

	private TextMessage receiveMessage() throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;

		logger.debug("First read:" + read);
		
		while (read != LINE_FEED && read != -1 && reading) {/* LF, error, drop */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
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
			
			/* only read valid characters, i.e. letters and numbers */
			// if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			// }
			
			/* stop reading if DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}

		logger.debug("Last read:" + read);
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		/* build final result */
		TextMessage msg = new TextMessage(msgBytes);
		
		logger.info("RECEIVE: STATUS=" 
			+ msg.getStatus() + " KEY=" + msg.getKey() + " VALUE=" + msg.getValue()
		);

		return msg;
    }

	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();

		logger.info("SEND: STATUS=" 
			+ msg.getStatus() + " KEY=" + msg.getKey() + " VALUE=" + msg.getValue()
		);
    }

	private void printError(String error) {
		System.out.println(PROMPT + "Error! " +  error);
	}
}
