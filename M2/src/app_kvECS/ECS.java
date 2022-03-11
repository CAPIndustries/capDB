package app_kvECS;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Collection;
import java.util.Scanner;
import java.util.Date;
import java.util.Stack;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.HashMap;
import java.util.ArrayList;

import java.text.SimpleDateFormat;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.PrintWriter;

import java.security.MessageDigest;

import java.math.BigInteger;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.CreateMode;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import logger.LogSetup;
import app_kvServer.IKVServer.CacheStrategy;

import app_kvECS.ZooKeeperWatcher;

import ecs.ECSNode;
import ecs.IECSNode;
import ecs.IECSNode.NodeEvent;

import exceptions.InvalidMessageException;

// Code citation: https://stackoverflow.com/a/943963

public class ECS implements IECSClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECS> ";
    private static boolean shutdown = false;
    private boolean running = false;
    private static TreeMap<String, ECSNode> active_servers = new TreeMap<String, ECSNode>();
    private Stack<String> available_servers = new Stack<String>();
    private HashMap<String, String> movedServers = new HashMap<String, String>();
    private int zkPort;
    private int port;
    private String rawMetadata = "";
    private ServerSocket serverSocket;

    public static ZooKeeper _zooKeeper = null;
    public static String _rootZnode = "/servers";

    /**
     * Main entry point for the ECS application.
     * 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
            new LogSetup("logs/ecs_" + fmt.format(new Date()) + ".log", Level.INFO, true);
            if (args.length != 3) {
                logger.error("Error! Invalid number of arguments!");
                logger.error("Usage: ECS <ECS port> <ZooKeeper port> <config file>!");
                System.exit(1);
            } else {
                int port = Integer.parseInt(args[0]);
                int zkPort = Integer.parseInt(args[1]);
                String config = args[2];
                ECS app = new ECS(port, zkPort, config);
                final Thread mainThread = Thread.currentThread();
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        try {
                            terminate();
                            // Delete the ZooKeeper root node
                            _zooKeeper.delete(_rootZnode, _zooKeeper.exists(_rootZnode,
                                    false).getVersion());
                        } catch (Exception e) {
                            logger.error("Error while completing the shutdown");

                            StringWriter sw = new StringWriter();
                            PrintWriter pw = new PrintWriter(sw);
                            e.printStackTrace(pw);
                            logger.error(sw.toString());
                        }
                    }
                });
                app.run();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    public ECS(int port, int zkPort, String config) {
        try {
            this.port = port;
            this.zkPort = zkPort;
            File myObj = new File(config);
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                available_servers.push(data);
            }
            Collections.shuffle(available_servers);
            // TODO:
            // 1. Create a stack of unused servers
            // Use a stack because pop is O(1)
            // 2. Shuffle this set
            // 3. When adding a new node, we can just pop the first/last element meaning
            // it'll
            // always be random
            // since the array was shuffled at the start. This is an optimization (reduces
            // later
            // calls to random)
            myReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void run() {
        try {
            initServer();
            logger.info("Initialized Server");
            initZooKeeper();
            logger.info("Initialized ZooKeeper");
        } catch (Exception e) {
            printError("Cannot start ZooKeeper!");
            logger.fatal("Cannot start ZooKeeper!");
            System.exit(1);
        }
        logger.info("Running ...");

        setRunning(true);
        while (isRunning()) {
            try {
                Socket client = serverSocket.accept();
                ClientConnection connection = new ClientConnection(client, this);
                new Thread(connection).start();

                logger.info("Connected to " + client.getInetAddress().getHostAddress() + ":"
                        + client.getPort());
            } catch (IOException e) {
                logger.error("Error! " + "Unable to establish connection. \n", e);
            }
        }
    }

    private void initServer() {
        logger.info("Initializing server ...");

        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " + serverSocket.getLocalPort());
        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());

            System.exit(1);
        }
    }

    private void initZooKeeper() throws Exception {
        logger.info("Initializing Zoo Keeper");
        _zooKeeper = new ZooKeeper("localhost:" + zkPort, 2000, new ZooKeeperWatcher(this));

        // Create the root node
        byte[] data = "".getBytes();
        _zooKeeper.create(_rootZnode, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        _zooKeeper.getChildren(_rootZnode,
                true);
    }

    @Override
    public boolean start() {
        try {
            for (IECSNode node : active_servers.values()) {
                String path = String.format("%s/%s", _rootZnode, node.getNodeName());

                // Only send a 'START' to the servers that are ready to be started (i.e. have
                // their metadata)
                byte[] recvData = _zooKeeper.getData(path,
                        false, null);
                NodeEvent status = NodeEvent.valueOf(new String(recvData,
                        "UTF-8").split("~")[0]);

                if (status == NodeEvent.METADATA_COMPLETE) {
                    logger.info("Going to start " + path);
                    byte[] data = NodeEvent.START.name().getBytes();
                    _zooKeeper.setData(path, data, _zooKeeper.exists(path, false).getVersion());
                }
            }

            return true;
        } catch (Exception e) {
            logger.error("Error starting server!");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
            return false;
        }
    }

    @Override
    public boolean stop() {
        logger.info("Broadcasting shutdown event to all participating servers ...");
        byte[] data = NodeEvent.STOP.name().getBytes();

        boolean res = broadcastData(data);
        if (!res) {
            logger.error("Could not broadcast STOP event!");
        }

        return res;
    }

    @Override
    public boolean shutdown() {
        try {
            logger.info("Shutting Down ...");

            // Send SHUTDOWN event to all participating servers
            byte[] data = NodeEvent.SHUTDOWN.name().getBytes();
            boolean res = broadcastData(data);
            if (!res) {
                logger.error("Could not broadcast SHUTDOWN event!");
                return false;
            }

            shutdown = true;
            completeShutdown();

            return true;
        } catch (Exception e) {
            logger.error(e);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
            return false;
        }
    }

    private static void terminate() {
        try {
            logger.info("Terminating program ...");

            // Send SHUTDOWN event to all participating servers
            byte[] data = NodeEvent.SHUTDOWN.name().getBytes();
            boolean res = broadcastData(data);
            if (!res) {
                logger.error("Could not broadcast SHUTDOWN event!");
                return;
            }

            shutdown = true;
            completeShutdown();
        } catch (Exception e) {
            logger.error(e);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
        }
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        logger.info("Attempting to add a node ...");
        try {
            if (available_servers.size() == 0) {
                logger.error("No more available servers!");
                return null;
            }
            String[] serverInfo = available_servers.pop().split("\\s+");

            String position = serverInfo[1] + ":" + serverInfo[2];
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(position.getBytes());
            byte[] digest = md.digest();

            BigInteger bi = new BigInteger(1, digest);
            String hash = String.format("%0" + (digest.length << 1) + "x", bi);
            // The lower bound will be NULL for now, but will fix it later once we're aware
            // of any other potential servers (cf. sendMetadata)
            String[] hashRange = { null, hash };
            ECSNode node = new ECSNode(serverInfo[0], serverInfo[1],
                    Integer.parseInt(serverInfo[2]), zkPort, hashRange);

            if (!node.initServer()) {
                logger.error("Could not SSH into server!");
            }

            logger.info("Added:" + serverInfo[1] + ":" + serverInfo[2]);
            active_servers.put(hash, node);

            // Check if any move events have to take place:
            String[] movedData = moveData(serverInfo[0], false);
            if (movedData != null) {
                logger.info("Have to move some data from: " + movedData[0] + " to " + movedData[3]);
                try {
                    movedServers.put(movedData[3], movedData[0]);
                    String data = NodeEvent.COPY.name() + "~"
                            + String.join(",", Arrays.copyOfRange(movedData, 1, movedData.length));

                    byte[] dataBytes = data.getBytes();
                    _zooKeeper.setData(movedData[0], dataBytes,
                            _zooKeeper.exists(movedData[0], false).getVersion());
                } catch (Exception e) {
                    logger.error("Error while sending move data");
                    logger.error(e.getMessage());
                }
            }

            return node;
        } catch (Exception e) {
            logger.error(e);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
            return null;
        }
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        logger.info("Attempting to add " + count + " nodes ...");

        ArrayList<IECSNode> nodes = new ArrayList<IECSNode>();
        for (int i = 0; i < count; ++i) {
            nodes.add(addNode(cacheStrategy, cacheSize));
        }

        return nodes;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        for (String name : nodeNames) {
            boolean res = removeNode(name);
            if (!res) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        try {
            if (available_servers.size() == 0) {
                return null;
            }

            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(Key.getBytes());
            byte[] digest = md.digest();

            BigInteger bi = new BigInteger(1, digest);
            String hash = String.format("%0" + (digest.length << 1) + "x", bi);

            return active_servers.get(hash);
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
            return null;
        }
    }

    private static boolean broadcastData(byte[] data) {
        // Broadcast to all servers
        try {
            for (IECSNode node : active_servers.values()) {
                String path = String.format("%s/%s", _rootZnode, node.getNodeName());
                if (_zooKeeper.exists(path, false) != null) {
                    _zooKeeper.setData(path, data, _zooKeeper.exists(path, false).getVersion());
                }
            }
            return true;
        } catch (Exception e) {
            logger.error("Error while shutting down");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
            return false;
        }
    }

    private static void completeShutdown() {
        try {
            if (active_servers.size() == 0) {
                logger.info("All remote servers closed. Shutting down ECS ....");
                logger.info(PROMPT + "All servers shut down!");
            } else {
                logger.info("Waiting on " + active_servers.size() + " to shutdown ...");
            }
        } catch (Exception e) {
            logger.error("Error while completing the shutdown");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
        }
    }

    // Remove a random server (currently set as the first element)
    public boolean removeNode() {
        if (active_servers.size() == 0) {
            logger.error("No servers running to delete!");
            return false;
        }

        Map.Entry<String, ECSNode> entry = active_servers.entrySet().iterator().next();
        return removeNode(entry.getValue().getNodeName());
    }

    // Remove a specific server
    public boolean removeNode(String serverName) {
        String path = String.format("%s/%s", _rootZnode, serverName);
        Map.Entry<String, ECSNode> entry = getEntry(serverName);
        if (entry != null) {
            String key = entry.getKey();
            ECSNode node = entry.getValue();
            try {
                // Check if any move events have to take place:
                String[] movedData = moveData(serverName, true);
                if (movedData != null) {
                    logger.info("Have to move some data from: " + movedData[0] + " to " + movedData[3]);
                    movedServers.put(movedData[3], movedData[0]);
                    try {
                        String data = NodeEvent.COPY.name() + "~"
                                + String.join(",", Arrays.copyOfRange(movedData, 1, movedData.length));

                        byte[] dataBytes = data.getBytes();
                        _zooKeeper.setData(movedData[0], dataBytes,
                                _zooKeeper.exists(movedData[0], false).getVersion());
                    } catch (Exception e) {
                        logger.error("Error while sending move data");
                        logger.error(e.getMessage());
                    }
                }

                if (_zooKeeper.exists(path, false) != null) {
                    byte[] data = NodeEvent.SHUTDOWN.name().getBytes();
                    _zooKeeper.setData(path, data, _zooKeeper.exists(path, false).getVersion());
                }
                logger.info("Removed " + serverName);
                active_servers.remove(key);

                sendMetadata();

                return true;
            } catch (Exception e) {
                logger.error("Error while removing node!");

                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                logger.error(sw.toString());

            }
        }

        logger.error("Could not find the node for removal");
        return false;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean nodeRemovedCreated() {
        Iterator<Map.Entry<String, ECSNode>> iter = active_servers.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<String, ECSNode> entry = iter.next();

            String key = entry.getKey();
            ECSNode node = entry.getValue();
            String path = String.format("%s/%s", _rootZnode, node.getNodeName());
            try {
                if (_zooKeeper.exists(path, false) == null) {
                    // Node was deleted
                    logger.info("Node removal of " + node.getNodeName());
                    iter.remove();

                    // TODO: Recalc the ring and hash and broadcast once more

                    if (shutdown) {
                        completeShutdown();
                    }
                } else {
                    byte[] recvData = _zooKeeper.getData(path,
                            false, null);
                    NodeEvent status = NodeEvent.valueOf(new String(recvData,
                            "UTF-8").split("~")[0]);

                    if (status == NodeEvent.BOOT) {
                        // Node was created
                        logger.info("Server still booting:" + path);
                        // Subscribe to it
                        _zooKeeper.exists(path,
                                true);
                        sendMetadata(path);
                    }
                }
            } catch (Exception e) {
                logger.error("Error while checking for removed nodes");

                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                logger.error(sw.toString());

                return false;
            }
        }

        updateMetadata();
        return true;
    }

    private Map.Entry<String, ECSNode> getEntry(String serverName) {
        for (Map.Entry<String, ECSNode> entry : active_servers.entrySet()) {
            ECSNode node = entry.getValue();

            if (node.getNodeName().equals(serverName)) {
                return entry;
            }
        }

        return null;
    }

    private String[] moveData(String serverName, boolean delete) {
        Map.Entry<String, ECSNode> entry = getEntry(serverName);

        if (entry != null) {
            String key = entry.getKey();
            ECSNode node = entry.getValue();

            Map.Entry<String, ECSNode> successor = active_servers.higherEntry(key);
            // If it wraps around:
            if (successor == null) {
                successor = active_servers.firstEntry();
            }
            ECSNode successorVal = successor.getValue();
            if (successorVal.getNodeName().equals(serverName)) {
                return null;
            }
            String lower = active_servers.lowerKey(key);
            if (lower == null) {
                lower = active_servers.lastKey();
            }

            if (delete) {
                String znode = String.format("%s/%s", _rootZnode, serverName);
                return new String[] { znode, lower, successor.getKey(), successorVal.getNodeName() };
            } else {
                String znode = String.format("%s/%s", _rootZnode, successorVal.getNodeName());
                return new String[] { znode, lower, key, serverName };
            }
        }

        return null;
    }

    private void updateMetadata() {
        // Update lower bounds now that all the participating servers have booted
        // Then, gather all server data
        List<String> serverData = new ArrayList();
        for (Map.Entry<String, ECSNode> entry : active_servers.entrySet()) {
            String key = entry.getKey();
            ECSNode node = entry.getValue();
            // Get the previous hash (or wrap around!)
            Map.Entry<String, ECSNode> prev = active_servers.lowerEntry(key);
            // Wrap around or if only participating server:
            if (prev == null) {
                prev = active_servers.lastEntry();
            }
            String[] hashRange = { prev.getKey(), key };
            node.setNodeHashRange(hashRange);
            // Update our internal info
            active_servers.put(key, node);
            serverData.add(node.getMeta());
        }
        rawMetadata = String.join(",", serverData);
    }

    public void sendMetadata(String path) {
        logger.info("Sending metadata to " + path + " ...");

        updateMetadata();

        String metadata = NodeEvent.METADATA.name() + "~" + rawMetadata;
        byte[] data = metadata.getBytes();
        logger.info("Sending:" + metadata);
        try {
            _zooKeeper.setData(path, data, _zooKeeper.exists(path, false).getVersion());
        } catch (Exception e) {
            logger.error("Error while broadcasting metadata");
            logger.error(e.getMessage());
        }
    }

    public void sendMetadata() {
        logger.info("Broadcasting metadata to all participating servers ...");
        updateMetadata();

        String metadata = NodeEvent.METADATA.name() + "~" + rawMetadata;
        byte[] data = metadata.getBytes();
        logger.info("Sending:" + metadata);
        // Broadcast to all servers
        try {
            for (IECSNode node : active_servers.values()) {
                String path = String.format("%s/%s", _rootZnode, node.getNodeName());
                logger.info("\tTo " + path);
                _zooKeeper.setData(path, data, _zooKeeper.exists(path, false).getVersion());
            }
        } catch (Exception e) {
            logger.error("Error while broadcasting metadata");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
        }
    }

    public void completeBoot(String serverName) {
        String movedServer = movedServers.get(serverName.substring(serverName.lastIndexOf("/") + 1));
        try {
            if (movedServer != null) {
                logger.info("Going to complete the move in: " + movedServer);
                byte[] data = NodeEvent.MOVE.name().getBytes();
                _zooKeeper.setData(movedServer, data, _zooKeeper.exists(movedServer, false).getVersion());

                movedServers.remove(serverName);
            }
        } catch (Exception e) {
            logger.error("Error while completing boot for: " + movedServer);

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
        }
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    public void shutDownECS() {
        try {
            if (shutdown()) {
                // Delete the ZooKeeper root node
                _zooKeeper.delete(_rootZnode, _zooKeeper.exists(_rootZnode,
                        false).getVersion());

                System.exit(0);
            } else {
                logger.error("Encountered error while shutting down!");
            }
        } catch (Exception e) {
            logger.error("Error while shutting down ECS");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.error(sw.toString());
        }
    }

    public String listNodes() {
        ArrayList<String> nodeList = new ArrayList<String>();
        for (Map.Entry<String, ECSNode> entry : active_servers.entrySet()) {
            ECSNode node = entry.getValue();
            String path = node.getNodeName();
            nodeList.add(path);
        }

        return String.join(",", nodeList);
    }
}
