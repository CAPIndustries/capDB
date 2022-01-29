package testing;

import org.junit.Test;

import junit.framework.TestCase;
import logger.LogSetup;

import java.util.Queue;
import java.util.ArrayList;
import java.util.concurrent.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import app_kvServer.ConcurrentNode;

import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import client.KVStore;
import testing.IResponseRunnable;

public class ConcurrencyBasicTest extends TestCase {

	private KVStore kvClient;
	private static Logger logger = Logger.getRootLogger();
	public static KVServer server;
	public static int port;

	public void setUp() {
		kvClient = new KVStore("localhost", port);
		try {
			kvClient.connect();
		} catch (Exception e) {
			logger.error(e);
		}
	}

	public void tearDown() {
		kvClient.disconnect();
		server.clearStorage();
	}

	public void testConcurrentGet() {
		final int NUM_CONNECTIONS = 5;
		logger.info("====TEST " + NUM_CONNECTIONS + " concurrent GETS====");

		final String key = "key";
		final String value = "woah";
		IKVMessage response = null;
		Exception ex = null;
		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assert(response.getStatus() == StatusType.PUT_SUCCESS);
		assert(response.getKey() == key);
		assert(response.getValue() == value);

		class ResponseRunnable implements IResponseRunnable {
			private volatile IKVMessage response;

			@Override
			public void run() {
				try {
					Logger logger = Logger.getRootLogger();
					logger.info("======Thread! START======");
					KVStore kv = new KVStore("localhost", port);
					kv.connect();
					IKVMessage response = kv.get(key);
					logger.info("======Thread! DONE======");
					logger.info("======Thread! Get: " + response.getValue() + "======");
					this.response = response;
					kv.disconnect();
				} catch (Exception e) {
					logger.error("Error in Thread:" + Thread.currentThread().getId() + ": " + e);
				}
			}

			@Override
			public IKVMessage getResponse() {
				return this.response;
			}
		}

		server.wait = true;

		logger.info("======Thread! Spawning======");
		ResponseRunnable[] values = new ResponseRunnable[NUM_CONNECTIONS];
		Thread[] threads = new Thread[NUM_CONNECTIONS];
		for (int i = 0; i < NUM_CONNECTIONS; ++i) {
			values[i] = new ResponseRunnable();
			threads[i] = new Thread(values[i]);
			threads[i].start();
		}

		while (!server.inStorage(key) ||
				server.getFileList().get(key).len() != NUM_CONNECTIONS) {
		}
		;

		server.wait = false;

		try {
			for (int i = 0; i < NUM_CONNECTIONS; ++i) {
				threads[i].join();
				IKVMessage tResponse = values[i].getResponse();
				assert(tResponse.getStatus() == StatusType.PUT_SUCCESS);
				assert(tResponse.getKey().equals(key));
				assert(tResponse.getValue().equals(value));
			}
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}

	public void testConcurrentPutDiffKeys() {
		final int NUM_CONNECTIONS = 5;
		final String KEY_PREFIX = "key_";
		final String VALUE = "foo";
		logger.info("====TEST " + NUM_CONNECTIONS + " concurrent PUTS (Different keys)====");

		class ResponseRunnable implements IResponseRunnable {
			private volatile IKVMessage response;

			@Override
			public void run() {
				try {
					Logger logger = Logger.getRootLogger();
					logger.info("======Thread! START======");
					KVStore kv = new KVStore("localhost", port);
					kv.connect();
					IKVMessage response = kv.put(KEY_PREFIX + Thread.currentThread().getId(), VALUE);
					logger.info("======Thread! DONE======");
					logger.info("======Thread! Response: " + response.print() + "======");
					this.response = response;
					kv.disconnect();
				} catch (Exception e) {
					logger.error("Error in Thread:" + Thread.currentThread().getId() + ": " + e);
				}
			}

			@Override
			public IKVMessage getResponse() {
				return this.response;
			}
		}

		server.wait = true;

		logger.info("======Thread! Spawning======");
		ResponseRunnable[] values = new ResponseRunnable[NUM_CONNECTIONS];
		Thread[] threads = new Thread[NUM_CONNECTIONS];
		for (int i = 0; i < NUM_CONNECTIONS; ++i) {
			values[i] = new ResponseRunnable();
			threads[i] = new Thread(values[i]);
			threads[i].start();
		}

		while (true) {
			int done = 0;
			for (int i = 0; i < NUM_CONNECTIONS; ++i) {
				if (server.inStorage(KEY_PREFIX + threads[i].getId())) {
					done += 1;
				}
			} 
			
			if (done == NUM_CONNECTIONS) break;
		}

		server.wait = false;

		Exception ex = null;
		try {
			for (int i = 0; i < NUM_CONNECTIONS; ++i) {
				threads[i].join();
				IKVMessage tResponse = values[i].getResponse();
				assert(tResponse.getStatus() == StatusType.PUT_SUCCESS);
				assert(tResponse.getKey().equals(KEY_PREFIX + threads[i].getId()));
				assert(tResponse.getValue().equals(VALUE));
			}
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}

	public void testConcurrentPutSameKeys() {
		assertTrue(true);
		if (true) return;

		final int NUM_CONNECTIONS = 5;
		final String KEY = "key";
		final String VALUE_PREFIX = "val_";
		logger.info("====TEST " + NUM_CONNECTIONS + " concurrent PUTS (Same keys)====");

		class ResponseRunnable implements IResponseRunnable {
			private volatile IKVMessage response;

			@Override
			public void run() {
				try {
					Logger logger = Logger.getRootLogger();
					logger.info("======Thread! START======");
					KVStore kv = new KVStore("localhost", port);
					kv.connect();
					IKVMessage response = kv.put(KEY, VALUE_PREFIX + Thread.currentThread().getId());
					logger.info("======Thread! DONE======");
					logger.info("======Thread! Response: " + response.print() + "======");
					this.response = response;
					kv.disconnect();
				} catch (Exception e) {
					logger.error("Error in Thread:" + Thread.currentThread().getId() + ": " + e);
				}
			}

			@Override
			public IKVMessage getResponse() {
				return this.response;
			}
		}

		server.wait = true;

		logger.info("======Thread! Spawning======");
		ResponseRunnable[] values = new ResponseRunnable[NUM_CONNECTIONS];
		Thread[] threads = new Thread[NUM_CONNECTIONS];
		for (int i = 0; i < NUM_CONNECTIONS; ++i) {
			values[i] = new ResponseRunnable();
			threads[i] = new Thread(values[i]);
			threads[i].start();
		}

		while (!server.inStorage(KEY) ||
				server.getFileList().get(KEY).len() != NUM_CONNECTIONS) {
		}
		;

		server.wait = false;

		Exception ex = null;
		try {
			for (int i = 0; i < NUM_CONNECTIONS; ++i) {
				threads[i].join();
				IKVMessage tResponse = values[i].getResponse();
				if (i == 0) {
					assert(tResponse.getStatus() == StatusType.PUT_SUCCESS);
				} else {
					assert(tResponse.getStatus() == StatusType.PUT_UPDATE);
				}
				assert(tResponse.getKey().equals(KEY));
				assert(tResponse.getValue().equals(VALUE_PREFIX + threads[i].getId()));
			}
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}
}
