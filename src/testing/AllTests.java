package testing;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.File;

import logger.LogSetup;

import app_kvServer.KVServer;
import app_kvServer.IKVServer.CacheStrategy;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

public class AllTests {
	private static KVServer kvserver;
	private static int PORT = 50000;

	static {
		try {
			File file = new File("logs/testing/test.log");
			file.delete();

			new LogSetup("logs/testing/test.log", Level.INFO);
			kvserver = new KVServer(PORT, 10, CacheStrategy.FIFO);
			kvserver.test = true;
			Runnable server = new Runnable() {
				@Override
				public void run() {
					kvserver.run();
				}
			};
			new Thread(server).start();
			// Originally was:
			// new KVServer(PORT, 10, CacheStrategy.FIFO);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");

		// ConnectionTest.port = PORT;
		// clientSuite.addTestSuite(ConnectionTest.class);
		
		// BasicTest.server = kvserver;
		// BasicTest.port = PORT;
		// clientSuite.addTestSuite(BasicTest.class);
		
		// ConcurrencyBasicTest.server = kvserver;
		// ConcurrencyBasicTest.port = PORT;
		// clientSuite.addTestSuite(ConcurrencyBasicTest.class);

		// ConcurrencyHardTest.server = kvserver;
		// ConcurrencyHardTest.port = PORT;
		// clientSuite.addTestSuite(ConcurrencyHardTest.class);

		// clientSuite.addTestSuite(AdditionalTest.class);

		return clientSuite;
	}
	// TODO Write a test case to check to make sure connection times out for a response
}
