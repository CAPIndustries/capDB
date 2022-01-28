package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import java.io.IOException;
import java.io.File;
import java.io.FileWriter;

import java.util.Date;
import java.util.Scanner;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.function.IntBinaryOperator;
import java.util.Iterator;

import java.text.SimpleDateFormat;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;
import app_kvServer.ConcurrentNode;

public class KVServer implements IKVServer {

	private static final String STORAGE_DIRECTORY = "storage/";
	private static final CacheStrategy START_CACHE_STRATEGY = CacheStrategy.LRU;
	private static final int START_CACHE_SIZE = 16;
	private static final int MAX_READS = 100;

	private static Logger logger = Logger.getRootLogger();
	private int port;
	private int cacheSize;
	private CacheStrategy strategy;
	private ServerSocket serverSocket;
	private boolean running;
	private File storageDirectory = new File(STORAGE_DIRECTORY);
	
	// TODO: I think the cache does indeed need to have concurrent access
	private LinkedHashMap<String, String> cache;
	// true = write in progress (locked) and false = data is accessible
	ConcurrentMap<String, ConcurrentNode> fileList = new ConcurrentHashMap<String, ConcurrentNode>();

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
	public KVServer(int port, final int cacheSize, CacheStrategy strategy) {
		logger.info("Creating server. Config: port=" + port + " Cache Size=" + cacheSize + " Caching strategy=" + strategy);
		
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;

		if (strategy == CacheStrategy.LRU) {
			cache = new LinkedHashMap<String, String>(cacheSize, 0.75f, true){
				protected boolean removeEldestEntry(Map.Entry eldest) {
					return size() > cacheSize;
				}
			};
		} else {
			logger.warn("Unimplemented caching strategy: " + strategy);
		}

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
		if (cache != null) {
			return cache.containsKey(key);
		}
		return false;
	}

	@Override
    public void clearCache() {
		if (cache != null) {
			cache.clear();
		}
	}

	@Override
	public boolean inStorage(String key) {
		// TODO: See if someone is writing to fileList first
		return fileList.containsKey(key);
	}

	@Override
    public void clearStorage() {
		logger.info("Clearing storage ...");
		fileList.clear();
		clearCache();
		File[] allContents = storageDirectory.listFiles();
		logger.info("Deleting " + allContents.length + " records");
		if (allContents != null) {
			for (File file : allContents) {
				file.delete();
			}
		}
		logger.info("Storage cleared");
	}

	@Override
    public String getKV(String key) throws Exception {
		logger.info("GET for key=" + key);

		// Check if key exists
		if (!inStorage(key)) {
			logger.info("KEY does not exist");
			throw new Exception("Key does not exist");
		} else {
			// TODO: Key could exist here, but then gets deleted in another thread. HANDLE THIS
			System.out.println("looks chill");
			int[] node = { (int) Thread.currentThread().getId(), NodeOperation.READ.getVal() };
			fileList.get(key).addToQueue(node);
			System.out.println("looks okay");

			while (fileList.get(key).peek() != null && fileList.get(key).peek()[1] == NodeOperation.READ.getVal()) {
				try {
					System.out.println("file list1: " + fileList.get(key).peek());
					readMap.get(key).acquire();
					break;
				} catch (Exception e) {
					readMap.get(key).release();
					logger.error(e);
				}
			}

			System.out.println("over here");
			System.out.println("cnt:" + readMap.get(key).availablePermits());

			// See if marked for deletion:
			if (fileList.get(key).isDeleted()) {
				// TODO: Make the following method concurrent. Perhaps add it to the queue as a PRUNE op
				// This is because a READ op is NOT supposed to modify global state, but pruning DOES!
				pruneDelete(key);
				removeTopQueue(key);
				readMap.get(key).release();
				logger.info("KEY does not exist");
				throw new Exception("Key does not exist");
			}

			if (inCache(key)) {
				logger.debug("Cache hit!");
				removeTopQueue(key);
				readMap.get(key).release();
				return cache.get(key);
			}

			File file = new File(STORAGE_DIRECTORY + key);
			StringBuilder fileContents = new StringBuilder((int) file.length());
			String value;

			try (Scanner scanner = new Scanner(file)) {
				while (scanner.hasNextLine()) {
					fileContents.append(scanner.nextLine() + System.lineSeparator());
				}
				
				removeTopQueue(key);
				readMap.get(key).release();

				String val = fileContents.toString().trim();
				cache.put(key, val);
				logger.info("Value=" + val);
				return val;
			} catch (Error e) {
				removeTopQueue(key);
				readMap.get(key).release();
				logger.error(e);
			}
		}

		// TODO: Handle this better:
		return "Error";
	}

	@Override
    public void putKV(String key, String value) throws Exception {
		logger.info("PUT for key=" + key + " value=" + value);

		try {
			fileList.putIfAbsent(key, new ConcurrentNode());
			readMap.putIfAbsent(key, new Semaphore(MAX_READS));

			NodeOperation op = value.equals("null") ? NodeOperation.DELETE : NodeOperation.WRITE;

			// add thread to back of list for this key - add is thread safe
			int[] node = { (int) Thread.currentThread().getId(), op.getVal() };
			fileList.get(key).addToQueue(node);

			// wait (spin) until threads turn and no one is reading
			// TODO: If a key gets deleted, I think fileList.get(key) would no longer work
			do {
			} while (fileList.get(key).peek() != null
					&& (fileList.get(key).peek()[0] != ((int) Thread.currentThread().getId())
					|| readMap.get(key).availablePermits() != MAX_READS));

			if (value.equals("null")) {
				// Delete the key
				logger.info("Deleting record");
				// TODO: Mark it as deleted then loop to see if anybody is reading/writing to it
				// If a write is after, keep the row by unmarking it as deleted (since the operation cancels)
				// If a read is after, check the deleted flag, if its deleted, make sure to return key DNE
				//			Otherwise, return the key
				// If at any point the queue is empty, and the row is marked as deleted, THEN remove it from the fileList
				// Delete it from the cache as well!
				fileList.get(key).setDeleted(true);
				pruneDelete(key);
				// NOT YET:
				// fileList.remove(key);
				File file = new File(STORAGE_DIRECTORY + key);
				file.delete();
				// TODO: Do you have to return an error if the key DNE?
			} else {
				// Insert/replace the key
				logger.info("Inserting/updating record");
				// Reset the deleted flag
				fileList.get(key).setDeleted(false);
				cache.put(key, value);
				try {
					FileWriter myWriter = new FileWriter("storage/" + key);
					myWriter.write(value);
					myWriter.close();
				} catch (IOException e) {
					logger.error(e);
				}
			}

			// remove the top item from the list for this key
			removeTopQueue(key);
		} catch (Exception e) {
			logger.error(e);
		}
	}

	@Override
    public void run() {
		setRunning(initializeServer());
        
        if (serverSocket != null) {
	        while (isRunning()) {
	            try {
	                Socket client = serverSocket.accept();                
	                ClientConnection connection = new ClientConnection(client, this);
	                new Thread(connection).start();
	                
	                logger.info("Connected to " 
	                		+ client.getInetAddress().getHostAddress() 
	                		+  ":" + client.getPort());
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
		close();
	}

	@Override
    public void close() {
		logger.info("Closing server ...");
		setRunning(false);
		try {
			serverSocket.close();
			logger.info("Server closed");
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	/**
     * @param args contains the program's input args (here for signature purposes)
     */
    public static void main(String[] args) {
    	try {
			SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
			new LogSetup("logs/server_" + fmt.format(new Date()) + ".log", Level.ALL, true);
			if (args.length != 1) {
				logger.error("Error! Invalid number of arguments!");
				logger.error("Usage: Server <port>!");
				System.exit(1);
			} else {
				int port = Integer.parseInt(args[0]);
				// No need to use the run method here since the contructor is supposed to 
				// start the server on its own
				// TODO: Allow passing additional arguments from the command line:
				new KVServer(port, START_CACHE_SIZE, START_CACHE_STRATEGY);
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

	private void removeTopQueue(String key) {
		if (fileList.get(key).peek() != null) {
			fileList.get(key).remove();
		}
	}

	private void pruneDelete(String key) {
		if (fileList.get(key).isEmpty()) {
			fileList.remove(key);
			cache.remove(key);
		}
	}

	private boolean initializeServer() {
    	logger.info("Initializing server ...");
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
		logger.info("Initializing storage ...");

		try {
			logger.info("Checking for storage directory at " + storageDirectory.getCanonicalPath());
			// Ensure storage directory exists
			if (!storageDirectory.exists()){
				logger.info("Storage directory does not exist. Creating new directory.");
				storageDirectory.mkdir();
			} else {
				logger.info("Storage directory exists. Loading data ...");
				// Load all the data
				File[] listOfFiles = storageDirectory.listFiles();
				
				for (int i = 0; i < listOfFiles.length; i++) {
					if (listOfFiles[i].isFile()) {
						readMap.put(listOfFiles[i].getName(), new Semaphore(MAX_READS));
						fileList.put(listOfFiles[i].getName(), new ConcurrentNode());
					} 
				}
				logger.info("Data successfully loaded");
			}
		} catch (Exception e) {
			logger.error(e);
		}
	}

	private boolean isRunning() {
		return this.running;
	}

	public void setRunning(boolean run) {
		this.running = run;
	}
}
