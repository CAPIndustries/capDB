// package testing;

// import java.io.File;
// import java.util.Collection;

// import junit.framework.TestCase;

// import org.apache.log4j.Logger;

// import app_kvECS.ECSClient;
// import app_kvECS.IECSClient;

// import ecs.IECSNode;
// import ecs.ECSNode;

// import client.KVStore;

// import app_kvServer.KVServer;

// import shared.messages.IKVMessage;
// import shared.messages.IKVMessage.StatusType;
// import java.io.IOException;

// public class ECSTest extends TestCase {

//     private final String STORAGE_DIRECTORY = "storage/";

//     private KVStore kvClient;
//     private static Logger logger = Logger.getRootLogger();
//     public static KVServer server;
//     public static int port;

//     private static ECS ecs = null;
//     private Exception ex = null;

//     public void setUp() {
//         // kvClient = new KVStore("localhost", 50000);
//         // try {
//         // server.clearStorage();
//         // kvClient.connect();
//         // } catch (Exception e) {
//         // logger.error(e);
//         // }
//     }

//     public void tearDown() {
//         // kvClient.disconnect();
//         // server.clearStorage();
//     }

//     public void testNoConfig() throws IOException {
//         logger.info("Starting ECS test");
//         try {
//             ecs = new ECSClient(2181, "ksajhsja");
//             logger.info("Done ECS test");
//         } catch (Exception e) {
//             ex = e;
//         }
//         assertTrue(ecs.available_servers.size() == 0);
//     }

//     public void testNormalConfig() throws IOException {
//         logger.info("Starting ECS test");
//         try {
//             ecs = new ECSClient(2181, "/homes/s/solank23/ece419/capDB/M2/ecs.config");
//             logger.info("Done ECS test");
//         } catch (Exception e) {
//             ex = e;
//         }
//         assertTrue(ecs.available_servers.size() == 8);
//     }

//     // Won't add a node if no servers are provided in config
//     public void testAddNodeWithNoServers() throws IOException {
//         IECSNode res = null;

//         try {
//             ecs = new ECSClient(2181, "/homes/s/solank23/ece419/capDB/M2/test_cases/bad.config");
//             res = ecs.addNode("FIFO", 16);
//         } catch (Exception e) {
//             ex = e;
//         }
//         assertNull(res);
//     }

//     // Won't add nodes if no servers are provided in config
//     public void testAddNodesWithNoServers() throws IOException {
//         Collection<IECSNode> res = null;

//         try {
//             ecs = new ECSClient(2181, "/homes/s/solank23/ece419/capDB/M2/test_cases/bad.config");
//             res = ecs.addNodes(10, "FIFO", 16);
//         } catch (Exception e) {
//             ex = e;
//         }
//         for (IECSNode node : res) {
//             assertNull(node);
//         }
//     }

//     // Will now add node
//     public void testAddNodeWithCorrectServer() throws IOException {
//         IECSNode res = null;
//         long startTime = System.nanoTime();
//         try {
//             ecs = new ECSClient(2181, "/homes/s/solank23/ece419/capDB/M2/test_cases/test.config");
//             res = ecs.addNode("FIFO", 16);
//         } catch (Exception e) {
//             ex = e;
//         }
//         long endTime = System.nanoTime() - startTime;
//         logger.info(String.format("Time Required: %d", endTime));
//         assertNotNull(res);
//     }

//     // Will now add node
//     public void testAddNodeWithCorrectServer() throws IOException {
//     IECSNode res = null;

//     try {
//     ecs = new ECSClient(2181,
//     "/homes/s/solank23/ece419/capDB/M2/test_cases/test.config");
//     res = ecs.addNode("FIFO", 16);
//     } catch (Exception e) {
//     ex = e;
//     }
//     assertNotNull(res);
//     }

//     // Will now add node
//     public void testAddNodeWithCorrectServer() throws IOException {
//     IECSNode res = null;

//     try {
//     ecs = new ECSClient(2181,
//     "/homes/s/solank23/ece419/capDB/M2/test_cases/test.config");
//     res = ecs.addNode("FIFO", 16);
//     } catch (Exception e) {
//     ex = e;
//     }
//     assertNotNull(res);
//     }

//     public void testStartNode() throws IOException {
//         IECSNode res = null;

//         try {
//             ecs = new ECSClient(2181, "/homes/s/solank23/ece419/capDB/M2/test_cases/test.config");
//             res = ecs.addNode("FIFO", 16);
//         } catch (Exception e) {
//             ex = e;
//         }
//         assertNotNull(res);
//     }
// }
