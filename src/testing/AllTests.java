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
	static {
		try {
			File file = new File("logs/testing/test.log");
			file.delete();
			new LogSetup("logs/testing/test.log", Level.ALL);
			kvserver = new KVServer(50000, 10, CacheStrategy.FIFO);
			kvserver.test = true;
			Runnable server = new Runnable() {
				@Override
				public void run() {
					kvserver.run();
				}
			};
			new Thread(server).start();
			// Originally was:
			// new KVServer(50000, 10, CacheStrategy.FIFO);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");

		Logger logger = Logger.getRootLogger();
		logger.debug("Warn: starting clear storage");

		InteractionTest.server = kvserver;
		kvserver.clearStorage();
		clientSuite.addTestSuite(ConnectionTest.class);
		kvserver.clearStorage();
		clientSuite.addTestSuite(InteractionTest.class);
		kvserver.clearStorage();
		clientSuite.addTestSuite(AdditionalTest.class);
		kvserver.clearStorage();

		return clientSuite;
	}

}
