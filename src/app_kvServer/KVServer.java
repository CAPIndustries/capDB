package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import java.io.IOException;
import java.io.File;
import java.io.FileWriter;

import java.util.Scanner;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.function.IntBinaryOperator;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

public class KVServer implements IKVServer {

	private static Logger logger = Logger.getRootLogger();
	private static final String STORAGE_DIRECTORY = "storage/";
	private static final Integer MAX_READS = 100;
	private int port;
	private int cacheSize;
	private CacheStrategy strategy;
	private ServerSocket serverSocket;
	private boolean running;
	File storageDirectory = new File(STORAGE_DIRECTORY);

	// true = write in progress (locked) and false = data is accessible
	ConcurrentMap<String, Queue<Integer[]>> fileList = new ConcurrentHashMap<String, Queue<Integer[]>>();

	// semaphore map to allow multiple reads
	ConcurrentMap<String, Semaphore> readMap = new ConcurrentHashMap<String, Semaphore>();

	/**
	 * Start KV Server at given port
	 * 
	 * @param port      given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *                  to keep in-memory
	 * @param strategy  specifies the cache replacement strategy in case the cache
	 *                  is full and there is a GET- or PUT-request on a key that is
	 *                  currently not contained in the cache. Options are "FIFO",
	 *                  "LRU",
	 *                  and "LFU".
	 */
	public KVServer(int port, int cacheSize, CacheStrategy strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.run();
	}

	@Override
	public int getPort() {
		return this.port;
	}

	@Override
	public String getHostname() {
		return serverSocket.getInetAddress().getHostName();
	}

	@Override
	public CacheStrategy getCacheStrategy() {
		return this.strategy;
	}

	@Override
	public int getCacheSize() {
		return this.cacheSize;
	}

	@Override
	public boolean inCache(String key) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clearCache() {
		// TODO Auto-generated method stub
	}

	@Override
	public boolean inStorage(String key) {
		// TODO: See if someone is writing to fileList first
		return fileList.containsKey(key);
	}

	@Override
	public void clearStorage() {
		fileList.clear();
		File[] allContents = storageDirectory.listFiles();
		if (allContents != null) {
			for (File file : allContents) {
				file.delete();
			}
		}
	}

	@Override
	public String getKV(String key) throws Exception {
		System.out.println("GET this key: " + key);
		// Check if key exists
		if (!inStorage(key)) {
			throw new Exception("Key does not exist");
		} else {
			// fileList.put(key, true);
			// TODO - swap ints with ENUM - write - 1 and read -0
			System.out.println("looks chill");
			Integer[] node = { (int) Thread.currentThread().getId(), 0 };
			fileList.get(key).add(node);
			System.out.println("looks okay");

			while (fileList.get(key).peek() != null && fileList.get(key).peek()[1] != 1) {
				try {
					readMap.get(key).acquire();
					System.out.println("file list1: " + fileList.get(key).peek());
					fileList.get(key).remove();
					if (fileList.get(key).peek() == null)
						break;
					System.out.println("file list2: " + fileList.get(key).peek());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			System.out.println("over here");
			System.out.println("cnt:" + readMap.get(key).availablePermits());
			File file = new File(STORAGE_DIRECTORY + key);
			StringBuilder fileContents = new StringBuilder((int) file.length());
			String value;

			try (Scanner scanner = new Scanner(file)) {
				while (scanner.hasNextLine()) {
					fileContents.append(scanner.nextLine() + System.lineSeparator());
				}
				// fileList.put(key, false);
				if (fileList.get(key).peek() != null)
					fileList.get(key).remove();
				readMap.get(key).release();
				return fileContents.toString().trim();
			} catch (Error e) {
				if (fileList.get(key).peek() != null)
					fileList.get(key).remove();
				readMap.get(key).release();
				e.printStackTrace();
			}
		}
		return "Error";
	}

	@Override
	public void putKV(String key, String value) throws Exception {
		try {
			fileList.putIfAbsent(key, new ConcurrentLinkedQueue<Integer[]>());
			readMap.putIfAbsent(key, new Semaphore(MAX_READS));

			// add thread to back of list for this key - add is thread safe
			Integer[] node = { (int) Thread.currentThread().getId(), 1 };
			fileList.get(key).add(node);

			// wait until threads turn and no one is reading
			do {
			} while (fileList.get(key).peek() != null
					&& fileList.get(key).peek()[0] != ((int) Thread.currentThread().getId())
					&& readMap.get(key).availablePermits() != MAX_READS);

			if (value.equals("null")) {
				// Delete the key
				// TODO: Get a lock on the fileList since I'm updating/writing to it

				fileList.remove(key);
				File file = new File(STORAGE_DIRECTORY + key);
				file.delete();
				// TODO: Do you have to return an error if the key DNE?
			} else {
				// Insert/replace the key

				// fileList.put(key, true);
				try {
					FileWriter myWriter = new FileWriter("storage/" + key);
					myWriter.write(value);
					myWriter.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				// fileList.put(key, false);
			}

			// remove the top item from the list for this key
			if (fileList.get(key).peek() != null)
				fileList.get(key).remove();

		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	@Override
	public void run() {
		running = initializeServer();

		if (serverSocket != null) {
			while (isRunning()) {
				try {
					Socket client = serverSocket.accept();
					ClientConnection connection = new ClientConnection(client, this);
					new Thread(connection).start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+ " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	// TODO: Difference between kill and close?
	@Override
	public void kill() {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() {
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	/**
	 * Main entry point for the KVServer application.
	 * 
	 * @param args contains the program's input args (here for signature purposes)
	 */
	public static void main(String[] args) {
		try {
			new LogSetup("logs/server.log", Level.ALL);
			if (args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				// No need to use the run method here since the contructor is supposed to
				// start the server on its own
				// TODO: Allow passing additional arguments from the command line:
				new KVServer(port, 0, CacheStrategy.None);
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		initializeStorage();
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if (e instanceof BindException) {
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	private void initializeStorage() {
		logger.info("Initialize storage ...");
		// Ensure storage directory exists
		if (!storageDirectory.exists()) {
			storageDirectory.mkdir();
		} else {
			// Load all the data
			File[] listOfFiles = storageDirectory.listFiles();
			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					readMap.put(listOfFiles[i].getName(), new Semaphore(MAX_READS));
					fileList.put(listOfFiles[i].getName(), new ConcurrentLinkedQueue<Integer[]>());
				}
			}
		}
	}

	private boolean isRunning() {
		return this.running;
	}

	public void setRunning(boolean run) {
		this.running = run;
	}
}
