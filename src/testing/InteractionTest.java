package testing;

import org.junit.Test;

import junit.framework.TestCase;
import client.KVStore;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class InteractionTest extends TestCase {

	private KVStore kvClient;
	private static Logger logger = Logger.getRootLogger();

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

	@Test
	public void testPut() {
		String key = "foo2";
		String value = "bar2";
		IKVMessage response = null;
		Exception ex = null;
		logger.debug("======= starting put =======");
		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		logger.debug("======= starting put =======");
		logger.debug("put status: " + response.getStatus());
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}

	@Test
	public void testPutDisconnected() {
		kvClient.disconnect();
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

	@Test
	public void testUpdate() {
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

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}

	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";

		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}

	@Test
	public void testGet() {
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

		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}

	@Test
	public void testLatency(){ 
		long Get20Put80[100]; 
		long Get80Put20[100]; 
		long Get50Put50[100]; 
		
		String key = "foo";
		String value = "bar";
		
		// place holder to be over written by many start times
		long startTime; 
		
		// initialize the test value in storage 
		kvClient.put(key, value);	
	
		// Get20Put80
		for (int i = 0; i < 100; i++){
			if (i % 5 == 0){
				startTime = System.nanoTime(); 
				kvClient.get(key);
			}
			else{ 
				startTime = System.nanoTime(); 
				kvClient.put(key, value);
			}
			Get20Put80[i] = System.nanoTime() - startTime;
		}

		// Get80Put20
		for (int i = 0; i < 100; i++){
			if (i % 5 == 0){
				startTime = System.nanoTime(); 
				kvClient.put(key, value);
		
			}
			else{ 
				startTime = System.nanoTime(); 
				kvClient.get(key);
			}
			Get80Put20[i] = System.nanoTime() - startTime;
		}	

		// Get50Put50
		for (int i = 0; i < 100; i++){
			if (i % 2 == 0){
				startTime = System.nanoTime(); 
				kvClient.get(key);
			}
			else{ 
				startTime = System.nanoTime(); 
				kvClient.put(key, value);
			}
			Get50Put50[i] = System.nanoTime() - startTime;
		}	
	}
}
