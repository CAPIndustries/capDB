package testing;

import org.junit.Test;

import junit.framework.TestCase;
import client.KVStore;
import app_kvServer.KVServer;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import logger.LogSetup;
import java.util.concurrent.*;
import java.util.Queue;

import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import app_kvServer.ConcurrentNode;

public class InteractionTest extends TestCase {

	private KVStore kvClient;
	private static Logger logger = Logger.getRootLogger();
	public static KVServer server;

	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}

	// @Test
	// public void testPut() {
	// String key = "foo2";
	// String value = "bar2";
	// IKVMessage response = null;
	// Exception ex = null;
	// logger.debug("======= starting put =======");
	// try {
	// response = kvClient.put(key, value);
	// } catch (Exception e) {
	// ex = e;
	// }
	// logger.debug("======= starting put =======");
	// logger.debug("put status: " + response.getStatus());
	// assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	// }

	// @Test
	// public void testPutDisconnected() {
	// kvClient.disconnect();
	// String key = "foo";
	// String value = "bar";
	// Exception ex = null;

	// try {
	// kvClient.put(key, value);
	// } catch (Exception e) {
	// ex = e;
	// }

	// assertNotNull(ex);
	// }

	// @Test
	// public void testUpdate() {
	// String key = "updateTestValue";
	// String initialValue = "initial";
	// String updatedValue = "updated";

	// IKVMessage response = null;
	// Exception ex = null;

	// try {
	// kvClient.put(key, initialValue);
	// response = kvClient.put(key, updatedValue);

	// } catch (Exception e) {
	// ex = e;
	// }

	// assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
	// && response.getValue().equals(updatedValue));
	// }

	// @Test
	// public void testDelete() {
	// String key = "deleteTestValue";
	// String value = "toDelete";

	// IKVMessage response = null;
	// Exception ex = null;

	// try {
	// kvClient.put(key, value);
	// response = kvClient.put(key, "null");

	// } catch (Exception e) {
	// ex = e;
	// }

	// assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	// }

	// @Test
	// public void testGet() {
	// String key = "foo";
	// String value = "bar";
	// IKVMessage response = null;
	// Exception ex = null;

	// try {
	// kvClient.put(key, value);
	// response = kvClient.get(key);
	// } catch (Exception e) {

	// ex = e;
	// }

	// assertTrue(ex == null && response.getValue().equals("bar"));
	// }

	// @Test
	// public void testGetUnsetValue() {
	// String key = "an unset value";
	// IKVMessage response = null;
	// Exception ex = null;

	// try {
	// response = kvClient.get(key);
	// } catch (Exception e) {
	// ex = e;
	// }

	// assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	// }

	@Test
	public void testConcurrentGet() {
		String key = "key";
		String value = "woah";
		IKVMessage response = null;
		Exception ex = null;
		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assert (response.getStatus() == StatusType.PUT_SUCCESS);
		assert (response.getKey() == key);
		assert (response.getValue() == value);

		// ArrayList<String> res_val = new ArrayList<String>();
		logger.info("====Thread!======");
		Runnable get = new Runnable() {
			@Override
			public void run() {
				try {
					KVStore kv = new KVStore("localhost", 50000);
					Logger logger = Logger.getRootLogger();
					kv.connect();
					logger.info("====Thread! START ====");
					logger.info(kv);
					String key = "key";
					IKVMessage response = kv.get(key);
					logger.info("====Thread! DONE ====");
					logger.info("====Thread! -> res: ====" + response.getValue());
					kv.disconnect();
				} catch (Exception e) {
					Exception ex = e;
					e.printStackTrace();
					logger.error("Error in THREADS:" + ex);
				}
			}
		};

		Thread[] tarr = new Thread[5];
		server.wait = true;
		for (int i = 0; i < 5; ++i) {
			tarr[i] = new Thread(get);
			tarr[i].start();
		}

		while (server.getFileList().get(key).len() != 5) {
			logger.debug(server.getFileList().get(key).len());
		}
		;

		// logger.debug("!!!!===DONE===!!!!");
		ConcurrentMap<String, ConcurrentNode> newList = server.getFileList();

		// newList.get(key);
		server.wait = false;
		logger.debug("!!!!===DONE===!!!!");

		for (int i = 0; i < 10; ++i) {
			try {
				tarr[i].join();
			} catch (Exception e) {
				logger.error(e);
			}
		}

		assertTrue(ex != null);
	}

	// TODO check to make sure connection times out

}
