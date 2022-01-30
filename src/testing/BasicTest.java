package testing;

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

	// public void testPut() {
	// 	logger.info("====TEST PUT====");
	// 	String key = "foo";
	// 	String value = "bar";

	// 	IKVMessage response = null;
	// 	Exception ex = null;
		
	// 	try {
	// 		response = kvClient.put(key, value);
	// 	} catch (Exception e) {
	// 		ex = e;
	// 	}

	// 	assertNull(ex);
	// 	assert(response.getStatus() == StatusType.PUT_SUCCESS);
	// 	assert(response.getKey().equals(key));
	// 	assert(response.getValue().equals(value));
	// }

	// public void testUpdate() {
	// 	logger.info("====TEST PUT UPDATE====");
	// 	String key = "updateTestValue";
	// 	String initialValue = "initial";
	// 	String updatedValue = "updated";

	// 	IKVMessage response = null;
	// 	Exception ex = null;

	// 	try {
	// 		kvClient.put(key, initialValue);
	// 		response = kvClient.put(key, updatedValue);
	// 	} catch (Exception e) {
	// 		ex = e;
	// 	}

	// 	assertNull(ex);
	// 	assertTrue(response.getStatus() == StatusType.PUT_UPDATE);
	// 	assertTrue(response.getKey().equals(key));
	// 	assertTrue(response.getValue().equals(updatedValue));
	// }

	// public void testDelete() {
	// 	logger.info("====TEST PUT DELETE====");
	// 	String key = "deleteTestValue";
	// 	String value = "toDelete";

	// 	IKVMessage response = null;
	// 	Exception ex = null;

	// 	try {
	// 		kvClient.put(key, value);
	// 		response = kvClient.put(key, "null");
	// 	} catch (Exception e) {
	// 		ex = e;
	// 	}

	// 	assertNull(ex);
	// 	assertTrue(response.getStatus() == StatusType.DELETE_SUCCESS);
	// 	assertTrue(response.getKey().equals(key));
	// }

	// public void testPutDisconnected() {
	// 	logger.info("====TEST PUT DISCONNECTED====");
	// 	tearDown();

	// 	String key = "foo";
	// 	String value = "bar";
	// 	Exception ex = null;

	// 	try {
	// 		kvClient.put(key, value);
	// 	} catch (Exception e) {
	// 		ex = e;
	// 	}

	// 	assertNotNull(ex);
	// }

	// public void testGet() {
	// 	logger.info("====TEST GET====");
	// 	String key = "foo";
	// 	String value = "bar";
	// 	IKVMessage response = null;
	// 	Exception ex = null;

	// 	try {
	// 		kvClient.put(key, value);
	// 		response = kvClient.get(key);
	// 	} catch (Exception e) {
	// 		ex = e;
	// 	}

	// 	assertNull(ex);
	// 	assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
	// 	assertTrue(response.getKey().equals(key));
	// 	assertTrue(response.getValue().equals(value));
	// }

	// public void testGetUnsetValue() {
	// 	logger.info("====TEST GET UNSET====");
	// 	String key = "an_unset_value";
	// 	IKVMessage response = null;
	// 	Exception ex = null;

	// 	try {
	// 		response = kvClient.get(key);
	// 	} catch (Exception e) {
	// 		ex = e;
	// 	}

	// 	assertNull(ex);
	// 	assertTrue(response.getStatus() == StatusType.GET_ERROR);
	// 	assertTrue(response.getKey().equals(key));
	// }

	public void testLatencyAndThroughput(){ 
		long Put2080[] = new long[100]; 
		long Put8020[] = new long[100]; //80 get 20 put
		long Put5050[] = new long[100]; 
		long Get2080[] = new long [100]; 
		long Get8020[] = new long [100]; //80 get 20 put 
		long Get5050[] = new long [100]; 
				
		// testing 
		String key = "foo";
		String value = "bar";
		
		// place holder to be over written by many start times
		long startTime; 
		
		// initialize the test value in storage 
		try {
			kvClient.put(key, value);	
		} catch (Exception e) {} 
			
		// 20 get 80 put 
		for (int i = 0; i < 100; i++){
			if (i % 5 == 0){
				startTime = System.nanoTime(); 

				try	{
					kvClient.get(key);
				} catch (Exception e) {}

				Get2080[i] = System.nanoTime() - startTime;
			}
			else{ 
				startTime = System.nanoTime(); 
				
				try {
				kvClient.put(key, value);
				} catch (Exception e) {} 
				
				Put2080[i] = System.nanoTime() - startTime;
			}
		}


		//  80 get 20 put
		for (int i = 0; i < 100; i++){
			if (i % 5 == 0) {
				startTime = System.nanoTime(); 
				try {
					kvClient.put(key, value);
				} catch (Exception e) {} 
				Put8020[i] = System.nanoTime() - startTime;
				logger.info("8020: put value" + Put8020[i]);
			}
			else{ 
				startTime = System.nanoTime(); 
				
				try {
					kvClient.get(key);
				} catch (Exception e) {}
				Get8020[i] = System.nanoTime() - startTime;
				logger.info("8020: get value" + Get8020[i]);
			}
		
		}	

		// Get50Put50
		for (int i = 0; i < 100; i++){
			if (i % 2 == 0){
				startTime = System.nanoTime(); 
				try {
					kvClient.get(key);
				} catch (Exception e) {}
				Get5050[i] = System.nanoTime() - startTime;
			}
			else{ 
				startTime = System.nanoTime(); 
				try {
					kvClient.put(key, value);
				} catch (Exception e) {} 
				Put5050[i] = System.nanoTime() - startTime; 
			}
		}


		// Calculations for averages
		long total = 0; 
 
		for(int i=0; i<Put2080.length; i++){
        		total = total + Put2080[i];
        	}
		double Put2080average = total / Put2080.length;
		
		total = 0; 

		for(int i=0; i<Get2080.length; i++){
        		total = total + Get2080[i];
        	}
		double Get2080average = total / Get2080.length;
		
		total = 0; 

		for(int i=0; i<Put8020.length; i++){
        		total = total + Put8020[i];
        	}
		double Put8020average = total / Put8020.length;

		total = 0; 

		for(int i=0; i<Get8020.length; i++){
        		total = total + Get8020[i];
        	}
		double Get8020average = total / Get8020.length;

		total = 0; 

		for(int i=0; i<Put5050.length; i++){
        		total = total + Put5050[i];
        	}
		double Put5050average = total / Put5050.length;

		total = 0; 

		for(int i=0; i<Get5050.length; i++){
        		total = total + Get5050[i];
        	}
		double Get5050average = total / Get5050.length;


		logger.info("20 Get 80 Put | Put latency: " + Put2080average); 
		logger.info("20 Get 80 Put | Get latency: " + Get2080average); 
		logger.info("80 Get 20 Put | Put latency: " + Put8020average); 
		logger.info("80 Get 20 Put | Get latency: " + Get8020average); 
		logger.info("50 Get 50 Put | Put latency: " + Put5050average); 
		logger.info("50 Get 50 Put | Get latency: " + Get5050average);
	}

	// public void concurrentLatency(){ 
		
	// 	Runnable fiftyfifty = new Runnable() {
	// 		@Override
	// 		public void run() {
	// 			try {
	// 				// setup
	// 				KVStore kv = new KVStore("localhost", 50000);
	// 				Logger logger = Logger.getRootLogger();
	// 				kv.connect();
					
	// 				logger.info("====Thread! START ====");
	// 				logger.info(kv);
					
	// 				String key = "key";
	// 				IKVMessage response = kv.get(key);
					
	// 				logger.info("====Thread! DONE ====");
	// 				logger.info("====Thread! -> res: ====" + response.getValue());
	// 				kv.disconnect();
	// 			} catch (Exception e) {
	// 				Exception ex = e;
	// 				e.printStackTrace();
	// 				logger.error("Error in THREADS:" + ex);
	// 			}
	// 		}
	// 	};

	// 	Runnable eightytwenty = new Runnable() {
	// 		@Override
	// 		public void run() {
	// 			try {
	// 				// setup 
	// 				KVStore kv = new KVStore("localhost", 50000);
	// 				Logger logger = Logger.getRootLogger();
	// 				kv.connect();

	// 				logger.info("====Thread PUT! START ====");
	// 				logger.info(kv);
					
					
	// 				String key = "key";
	// 				IKVMessage response = kv.put(key, "" + (int) Thread.currentThread().getId());


	// 				logger.info("====Thread! DONE ====");
	// 				logger.info("====Thread! -> res: ====" + response.getStatus());

	// 				kv.disconnect();
				
	// 			} catch (Exception e) {
	// 				Exception ex = e;
	// 				e.printStackTrace();
	// 				logger.error("Error in THREADS:" + ex);
	// 			}
	// 		}
	// 	};

	// 	Runnable twentyeighty  = new Runnable() {
	// 		@Override
	// 		public void run() {
	// 			try {
	// 				// setup 
	// 				KVStore kv = new KVStore("localhost", 50000);
	// 				Logger logger = Logger.getRootLogger();
	// 				kv.connect();

	// 				logger.info("====Thread PUT! START ====");
	// 				logger.info(kv);
					
					
	// 				String key = "key";
	// 				IKVMessage response = kv.put(key, "" + (int) Thread.currentThread().getId());


	// 				logger.info("====Thread! DONE ====");
	// 				logger.info("====Thread! -> res: ====" + response.getStatus());

	// 				kv.disconnect();
				
	// 			} catch (Exception e) {
	// 				Exception ex = e;
	// 				e.printStackTrace();
	// 				logger.error("Error in THREADS:" + ex);
	// 			}
	// 		}
	// 	};

	// 	int t_count = 10;
	// 	Thread[] tarr = new Thread[t_count];
	// 	for (int i = 0; i < t_count; ++i) {
	// 		if (i % 5 == 0) {
	// 			tarr[i] = new Thread(put);
	// 			tarr[i].start();
	// 		} else {
	// 			tarr[i] = new Thread(get);
	// 			tarr[i].start();
	// 		}
	// 	}
	// }
}
