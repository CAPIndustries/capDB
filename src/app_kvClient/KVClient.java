package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import shared.messages.KVMessage;

import logger.LogSetup;

import client.KVStore;
import client.KVCommInterface;

// TODO: Use the logger more extensively

public class KVClient implements IKVClient {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVClient> ";
	private BufferedReader stdin;
	private KVStore store;
    private boolean stop = false;
	private int serverPort;

    @Override
    public void newConnection(String hostname, int port) throws UnknownHostException, IOException {
		store = new KVStore(hostname, port);
		store.connect();
    }

    @Override
    public KVCommInterface getStore() {
		return store;
    }

	/**
     * Main entry point for the KVClient application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.OFF);
			KVClient app = new KVClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }

    public void run() {
		// TODO: for debugging (delete later):
		try {
			if (store == null) {
				this.newConnection("127.0.0.1", 9696);
			}
		} catch (Exception e) {
		}
		
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}

    private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		if(tokens[0].equals("connect")) {	
			if(tokens.length == 3) {
				connectCommand(tokens[1], tokens[2]);
			} else {
				printError("Expected 2 arguments: connect <address> <port>");
			}
		} else if (tokens[0].equals("disconnect")  && tokens.length == 1) {
			disconnect();
		} else if (tokens[0].equals("put")) {
			if(tokens.length >= 3) {
				if(store != null && store.isRunning()){
					StringBuilder msg = new StringBuilder();
					for(int i = 2; i < tokens.length; i++) {
						msg.append(tokens[i]);
						if (i != tokens.length - 1) {
							msg.append(" ");
						}
					}

					putCommand(tokens[1], msg.toString());
				} else {
					printError("Not connected!");
				}
			} else {
				printError("Expected 2 arguments: put <key> <value>");
			}
		} else if (tokens[0].equals("get")) {
			if(tokens.length == 2) {
				if(store != null && store.isRunning()){
					getCommand(tokens[1]);
				} else {
					printError("Not connected!");
				}
			} else {
				printError("Expected 1 argument: get <key>");
			}
		} else if (tokens[0].equals("logLevel")) {
			if (tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Expected 1 argument: logLevel <level>");
			}
		} else if (tokens[0].equals("help")) {
			if (tokens.length == 1) {
				printHelp();
			} else {
				printError("Expected 0 arguments");
			}
		} else if (tokens[0].equals("quit")){
			if (tokens.length == 1) {
				stop = true;
				disconnect();
				System.out.println(PROMPT + "Application exit!");
			} else {
				printError("Expected 0 arguments");
			}
		} else {
			printError("Unknown command");
			printHelp();
		}
	}

	private void connectCommand(String hostname, String port) {
		try {
			this.newConnection(hostname, Integer.parseInt(port));
		} catch(NumberFormatException nfe) {
			printError("No valid address. Port must be a number!");
			logger.info("Unable to parse argument <port>", nfe);
		} catch (UnknownHostException e) {
			printError("Unknown Host!");
			logger.info("Unknown Host!", e);
		} catch (IOException e) {
			printError("Could not establish connection!");
			logger.warn("Could not establish connection!", e);
		}
	}

	private void putCommand(String key, String value) {
		try {
			KVMessage res = store.put(key, value);
			switch (res.getStatus()) {
				case PUT_SUCCESS:
					System.out.println("SUCCESS: " + key + " inserted");
					break;
				case PUT_UPDATE:
					String msg = value.equals("null") ? " deleted" : " updated";
					System.out.println("SUCCESS: " + key + msg);
					break;
				case PUT_ERROR:
					System.out.println("ERROR: PUT operation with key " + key + " failed: " + res.getValue());
					break;
				default:
					System.out.println("ERROR: Unknown status returned for PUT");
					break;
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e);
			printError("PUT Error!");
		}
	}

	private void getCommand(String key) {
		try {
			KVMessage res = store.get(key);
			switch (res.getStatus()) {
				case GET_SUCCESS:
					System.out.println("SUCCESS: " + key + " = " + res.getValue());
					break;
				case GET_ERROR:
					System.out.println("ERROR: GET operation with key " + key + " failed: " + res.getValue());
					break;
				default:
					System.out.println("ERROR: Unknown status returned for GET");
					break;
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e);
			printError("GET Error!");
		}
	}

    private void disconnect() {
		if (store != null) {
			store.closeConnection();
			store = null;
		}
	}

    private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("KV CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to the storage server based on the given server address and the port number of the storage service\n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server\n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t inserts a key-value pair into the storage server data structures, updates (overwrites) the current value with the given value if the server already contains the specified key or deletes the entry for the given key if <value> equals null\n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t retrieves the value for the given key from the storage server\n");
		
		
		sb.append(PROMPT).append("logLevel <level");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("help");
		sb.append("\t\t shows this help menu\n");
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}
	
	private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ LogSetup.getPossibleLogLevels());
	}

	private String setLevel(String levelString) {
		if (levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if (levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if (levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if (levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if (levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if (levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if (levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}
	
    private void printError(String error) {
		System.out.println(PROMPT + "Error! " +  error);
	}
}
