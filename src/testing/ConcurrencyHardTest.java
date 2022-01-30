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
		server.clearStorage();
	}
	
	public void testConcurrentDelete() {
		final int NUM_CONNECTIONS = 5;
		final String key = "key";
		final String value = "woah";
		logger.info("====TEST " + NUM_CONNECTIONS + " concurrent DELETES====");

		IKVMessage response = null;
		Exception ex = null;
		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
		assertTrue(response.getKey().equals(key));
		assertTrue(response.getValue().equals(value));

		class ResponseRunnable implements IResponseRunnable {
			private volatile IKVMessage response;
			private volatile int id;

			@Override
			public void run() {
				try {
					Logger logger = Logger.getRootLogger();
					logger.info("======Thread! START======");
					KVStore kv = new KVStore("localhost", port);
					kv.connect();
					this.id = id;
					IKVMessage response = kv.put(key, "null");
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

			@Override
			public int getID() {
				return this.id;
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

		while (server.getClientRequests().get(key).len() != NUM_CONNECTIONS) {
		}
		;

		logger.info("Queue: " + server.getClientRequests().get(key).printQ());

		server.wait = false;

		try {
			for (int i = 0; i < NUM_CONNECTIONS; ++i) {
				threads[i].join();
				IKVMessage tResponse = values[i].getResponse();
				assertTrue(tResponse.getKey().equals(key));
				logger.info("Thread " + threads[i].getId() + " got:" + tResponse.getStatus());
				if (i == 0) {
					assertTrue(tResponse.getStatus() == StatusType.DELETE_SUCCESS);
				} else {
					assertTrue(tResponse.getStatus() == StatusType.DELETE_ERROR);
				}
			}
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue(response.getStatus() == StatusType.GET_ERROR);
		assertTrue(response.getKey() == key);
	}
}
