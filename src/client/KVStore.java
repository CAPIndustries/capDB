package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.net.SocketTimeoutException;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

public class KVStore implements KVCommInterface {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVStore> ";
	private boolean running;
	private String address;
	private int port;
	private Socket clientSocket;
	private OutputStream output;
 	private InputStream input;
	private long lastResponse;
	ScheduledFuture<?> heartbeatThread;

	private static final int HEARTBEAT_INTERVAL = 1000;
	private static final int HEARTBEAT_TRANSMISSION = HEARTBEAT_INTERVAL * 10;
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
		clientSocket.setSoTimeout(HEARTBEAT_TRANSMISSION);
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
		setRunning(true);
		setLastResponse(System.currentTimeMillis());

		scheduleHeartbeat();
		
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
	public IKVMessage put(String key, String value) throws Exception {
		KVMessage msg = new KVMessage(key, value, StatusType.PUT);
		sendMessage(msg, false);
		
		KVMessage res = receiveMessage(false);
		
		return res;
	}
	
	@Override
	public IKVMessage get(String key) throws Exception {
		KVMessage msg = new KVMessage(key, null, StatusType.GET);
		sendMessage(msg, false);
		
		System.out.println("x");
		KVMessage res = receiveMessage(false);
		System.out.println("y");

		return res;
	}

	private void scheduleHeartbeat() {
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		heartbeatThread = scheduler.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				if (getLastResponse() + HEARTBEAT_TRANSMISSION < System.currentTimeMillis()) {
					byte msgBytes[] = { StatusType.HEARTBEAT.getVal() };
					KVMessage msg = new KVMessage(msgBytes);
					
					try {
						sendMessage(msg, true);
						KVMessage res = receiveMessage(true);
					} catch (Exception e) {
						disconnect("Server shutdown!");
					}
				}
			}
		}, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
	}

	public void disconnect(String msg) {
		try {
			heartbeatThread.cancel(false);
			System.out.println();
			System.out.println(PROMPT + msg);
			System.out.print("KVClient> ");
			logger.info(msg);
			setRunning(false);
			if (clientSocket != null) {
				input.close();
				output.close();
				clientSocket.close();
				clientSocket = null;
			}
		} catch (IOException ioe) {
			printError("Unable to close connection!");
			logger.error("Unable to close connection!", ioe);
		}
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean run) {
		running = run;
	}

	public long getLastResponse() {
		return lastResponse;
	}

	public synchronized void setLastResponse(long response) {
		lastResponse = response;
	}

	public synchronized void closeConnection() {
		disconnect();
	}

	private synchronized KVMessage receiveMessage(boolean heartbeat) throws IOException, Exception {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
		logger.debug("First read:" + read);

		if (read == -1) {
			throw new Exception("Reached end of stream!");
		}

		setLastResponse(System.currentTimeMillis());
		
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
		KVMessage msg = new KVMessage(msgBytes);
		
		if (!heartbeat) {
			logger.info("RECEIVE: STATUS=" 
				+ msg.getStatus() + " KEY=" + msg.getKey() + " VALUE=" + msg.getValue()
			);
		}

		return msg;
    }

	public synchronized void sendMessage(KVMessage msg, boolean heartbeat) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();

		if (!heartbeat) {
			logger.info("SEND: STATUS=" 
				+ msg.getStatus() + " KEY=" + msg.getKey() + " VALUE=" + msg.getValue()
			);
		}
    }

	private void printError(String error) {
		System.out.println(PROMPT + "Error! " +  error);
	}
}
