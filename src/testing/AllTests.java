package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import app_kvServer.IKVServer.CacheStrategy;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ALL);
			Runnable server = new Runnable() {
				@Override
				public void run() {
					new KVServer(50000, 10, CacheStrategy.None);
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
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(AdditionalTest.class);
		return clientSuite;
	}

}
