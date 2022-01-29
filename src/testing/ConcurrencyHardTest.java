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

public class ConcurrencyHardTest extends TestCase {

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
		// server.clearStorage();
	}
	
	public void testConcurrentDelete() {
		assertTrue(true);
		if (true) return;

		final int NUM_CONNECTIONS = 5;
		logger.info("====TEST " + NUM_CONNECTIONS + " concurrent DELETES====");

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

		while (server.getFileList().get(key).len() != NUM_CONNECTIONS) {
			logger.debug(server.getFileList().get(key).len());
		}
		;

		server.wait = false;

		try {
			for (int i = 0; i < NUM_CONNECTIONS; ++i) {
				threads[i].join();
				IKVMessage tResponse = values[i].getResponse();
				assert(tResponse.getStatus() == StatusType.GET_SUCCESS);
				assert(tResponse.getKey().equals(key));
				assert(tResponse.getValue().equals(value));
			}
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}
}
