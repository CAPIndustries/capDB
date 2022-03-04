package app_kvServer;

import shared.messages.KVMessage;

public interface IKVServer {
    public enum Status {
        STOPPED, LOCKED, STARTED
    };

    public enum CacheStrategy {
        None, LRU, LFU, FIFO
    };

    public enum NodeOperation {
        READ(0), WRITE(1), DELETE(2);

        private final int val;

        NodeOperation(int val) {
            this.val = val;
        }

        public int getVal() {
            return this.val;
        }
    };

    /**
     * Get the port number of the server
     * 
     * @return port number
     */
    public int getPort();

    /**
     * Get the hostname of the server
     * 
     * @return hostname of server
     */
    public String getHostname();

    /**
     * Get the cache strategy of the server
     * 
     * @return cache strategy
     */
    public CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     * 
     * @return cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in storage. NOTE: does not modify any other properties
     * 
     * @return true if key in storage, false otherwise
     */
    public boolean inStorage(String key);

    /**
     * Check if key is in storage. NOTE: does not modify any other properties
     * 
     * @return true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     * 
     * @return value associated with key
     * @throws Exception when key not in the key range of the server
     */
    public KVMessage getKV(int clientPort, String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * 
     * @throws Exception when key not in the key range of the server
     */
    public KVMessage putKV(int clientPort, String key, String value) throws Exception;

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    /**
     * Clear the storage of the server
     */
    public void clearStorage();

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions NOTE: this includes
     * performing saving
     * to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();
}
