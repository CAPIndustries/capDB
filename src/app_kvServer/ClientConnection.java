package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.*;

import shared.messages.KVMessage;
import shared.messages.TextMessage;
import shared.messages.KVMessage.StatusType;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	KVServer server;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer server) {
		this.clientSocket = clientSocket;
		this.server = server;
		this.isOpen = true;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			
			while(isOpen) {
				try {
					TextMessage latestMsg = receiveMessage();
					switch (latestMsg.getStatus()) {
						case PUT:
							System.out.println("PUT");
							System.out.println("Key:" + latestMsg.getKey());
							System.out.println("Value:" + latestMsg.getValue());
							TextMessage putRes = putKV(latestMsg.getKey(), latestMsg.getValue());
							sendMessage(putRes);
							break;
						case GET:
							System.out.println("GET");
							System.out.println("Key:" + latestMsg.getKey());
							TextMessage getRes = getKV(latestMsg.getKey());
							sendMessage(getRes);
							break;
						default:
							// TODO: Send an error to the user, probably need another StatusType value
							System.out.println("Status:" + latestMsg.getStatus());
							break;
					}
					
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	private TextMessage putKV(String key, String value) {
		TextMessage res;
		
		try {
			StatusType putStatus = server.inStorage(key) ? StatusType.PUT_UPDATE : StatusType.PUT_SUCCESS;
			server.putKV(key, value);

			res = new TextMessage(key, value, putStatus);
		} catch (Exception e) {
			//TODO: handle exception
			res = new TextMessage(key, value, StatusType.PUT_ERROR);
		}
		
		return res;
	}

	private TextMessage getKV(String key) {
		String value;
		TextMessage res;

		try {
			value = server.getKV(key);
			res = new TextMessage(key, value, StatusType.GET_SUCCESS);
		} catch (Exception e) {
			//TODO: handle exception
			res = new TextMessage(key, e.getMessage(), StatusType.GET_ERROR);
		}
		
		return res;
	}
	
	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		// logger.info("SEND \t<" 
		// 		+ clientSocket.getInetAddress().getHostAddress() + ":" 
		// 		+ clientSocket.getPort() + ">: '" 
		// 		+ msg.getMsg() +"'");
    }
	
	private TextMessage receiveMessage() throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;

		System.out.println("First read:" + read);

		while (read != LINE_FEED && read != -1 && reading) {/* LF, error, drop*/
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
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}

		System.out.println("Last read:" + read);
		// if (read == -1) {
		// 	System.out.println("Its -1");
		// 	isOpen = false;
		// }
		
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
		// logger.info("RECEIVE \t<" 
		// 		+ clientSocket.getInetAddress().getHostAddress() + ":" 
		// 		+ clientSocket.getPort() + ">: '" 
		// 		+ msg.getMsg().trim() + "'");
		return msg;
	}
}