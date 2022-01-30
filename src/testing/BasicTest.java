package testing;

import java.io.File;

import junit.framework.TestCase;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVStore;

import app_kvServer.KVServer;

import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import logger.LogSetup;

public class BasicTest extends TestCase {

	private KVStore kvClient;
	private final String STORAGE_DIRECTORY = "storage/";
	private static Logger logger = Logger.getRootLogger();
	public static KVServer server;
	public static int port;

	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
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

	public void testPut() {
		logger.info("====TEST PUT====");
		String key = "foo";
		String value = "bar";

		IKVMessage response = null;
		Exception ex = null;
		
		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue(new File(STORAGE_DIRECTORY + key).isFile());
		assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
		assertTrue(response.getKey().equals(key));
		assertTrue(response.getValue().equals(value));
	}

	public void testUpdate() {
		logger.info("====TEST PUT UPDATE====");
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";

		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue(new File(STORAGE_DIRECTORY + key).isFile());
		assertTrue(response.getStatus() == StatusType.PUT_UPDATE);
		assertTrue(response.getKey().equals(key));
		assertTrue(response.getValue().equals(updatedValue));
	}

	public void testDelete() {
		logger.info("====TEST PUT DELETE====");
		String key = "deleteTestValue";
		String value = "toDelete";

		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			assertTrue(new File(STORAGE_DIRECTORY + key).isFile());
			response = kvClient.put(key, "null");
			// Wait for pruning to complete:
			Thread.sleep(5);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertFalse(new File(STORAGE_DIRECTORY + key).isFile());
		assertTrue(response.getStatus() == StatusType.DELETE_SUCCESS);
		assertTrue(response.getKey().equals(key));
	}

	public void testPutDisconnected() {
		logger.info("====TEST PUT DISCONNECTED====");
		tearDown();

		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	public void testGet() {
		logger.info("====TEST GET====");
		String key = "foo";
		String value = "bar";
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
		assertTrue(response.getKey().equals(key));
		assertTrue(response.getValue().equals(value));
	}

	public void testGetUnsetValue() {
		logger.info("====TEST GET UNSET====");
		String key = "an_unset_value";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue(response.getStatus() == StatusType.GET_ERROR);
		assertTrue(response.getKey().equals(key));
	}
	
}
