package app_kvECS;

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

import java.text.SimpleDateFormat;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
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

// Code citation: https://stackoverflow.com/a/943963

public class ECSClient implements IECSClient {
    private static final String PROMPT = "ECSClient> ";
    private static final CacheStrategy DEFAULT_CACHE_STRATEGY = CacheStrategy.LRU;
    private static final int DEFAULT_CACHE_SIZE = 16;
    private static Logger logger = Logger.getRootLogger();

    private BufferedReader stdin;
    private boolean running = false;
    private Stack<String> available_servers = new Stack<String>();
    private TreeMap<String, ECSNode> active_servers = new TreeMap<String, ECSNode>();
    private String rawMetadata = "";
    private boolean shutdown = false;

    private int zkPort;
    public ZooKeeper _zooKeeper = null;
    public String _rootZnode = "/servers";

    /**
     * Main entry point for the KVClient application.
     * 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
            new LogSetup("logs/ecs_" + fmt.format(new Date()) + ".log", Level.ALL, false);
            if (args.length != 2) {
                logger.error("Error! Invalid number of arguments!");
                logger.error("Usage: ECS <ZooKeeper port> <config file>!");
                System.exit(1);
            } else {
                int zkPort = Integer.parseInt(args[0]);
                String config = args[1];
                ECSClient app = new ECSClient(zkPort, config);
                app.run();
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    public ECSClient(int zkPort, String config) {
        try {
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
            initZooKeeper();
        } catch (Exception e) {
            printError("Cannot start ZooKeeper!");
            logger.fatal("Cannot start ZooKeeper!");
            System.exit(1);
        }
        logger.info("Initialized Zoo Keeper");

        setRunning(true);
        while (isRunning()) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                handleCommand(cmdLine);
            } catch (IOException e) {
                setRunning(false);
                printError("CLI does not respond - Application terminated");
                logger.fatal("CLI does not respond - Application terminated");
            }
        }
    }

    private void handleCommand(String cmdLine) {
        // TODO: Ensure that certain commands are only accessible AFTER the ZK is
        // initialized
        if (cmdLine.trim().length() == 0) {
            return;
        }

        logger.info("User input: " + cmdLine);
        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("addNodes")) {
            if (tokens.length == 2) {
                addNodes(Integer.parseInt(tokens[1]), DEFAULT_CACHE_STRATEGY.name(),
                        DEFAULT_CACHE_SIZE);
            } else {
                printError("Expected 1 argument: addNodes <numberOfNodes>");
            }
        } else if (tokens[0].equals("start")) {
            if (tokens.length == 1) {
                start();
            } else {
                printError("Expected 0 arguments");
            }
        } else if (tokens[0].equals("stop")) {
            if (tokens.length == 1) {
                stop();
            } else {
                printError("Expected 0 arguments");
            }
        } else if (tokens[0].equals("shutDown")) {
            if (tokens.length == 1) {
                shutdown();
            } else {
                printError("Expected 0 arguments");
            }
        } else if (tokens[0].equals("addNode")) {
            if (tokens.length == 1) {
                addNode(DEFAULT_CACHE_STRATEGY.name(), DEFAULT_CACHE_SIZE);
            } else {
                printError("Expected 0 arguments");
            }
        } else if (tokens[0].equals("removeNode")) {
            if (tokens.length == 2) {
                if (!removeNode(tokens[1])) {
                    printError("Could not remove node!");
                }
            } else {
                printError("Expected 1 argument: removeNode <ZooKeeper path of server>");
            }
        } else if (tokens[0].equals("help")) {
            if (tokens.length == 1) {
                printHelp();
            } else {
                printError("Expected 0 arguments");
            }
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isRunning() {
        return running;
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
                } else if (status == NodeEvent.COPY_COMPLETE) {
                    logger.info("Going to move " + path);
                    byte[] data = NodeEvent.MOVE.name().getBytes();
                    _zooKeeper.setData(path, data, _zooKeeper.exists(path, false).getVersion());
                }
            }
        } catch (Exception e) {
            logger.error("Error starting server!");
            e.printStackTrace();
        }

        return false;
    }

    private boolean broadcastData(byte[] data) {
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
            e.printStackTrace();
            return false;
        }
    }

    private void completeShutdown() {
        try {
            if (active_servers.size() == 0) {
                logger.info("All remote servers closed. Shutting down ECS ....");
                // Delete the ZooKeeper root node
                _zooKeeper.delete(_rootZnode, _zooKeeper.exists(_rootZnode, false).getVersion());

                _zooKeeper.close();
                logger.info(PROMPT + "Application exit!");
                System.exit(0);
            } else {
                logger.info("Waiting on " + active_servers.size() + " to shutdown ...");
            }
        } catch (Exception e) {
            logger.error("Error while completing the shutdown");
            e.printStackTrace();
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
            String[] movedData = moveData(String.format("%s/%s", _rootZnode, serverInfo[0]));
            if (movedData != null) {
                logger.info("Have to move some data from: " + movedData[0] + " to " + movedData[3]);
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

            return node;
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
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
        for (Map.Entry<String, ECSNode> entry : active_servers.entrySet()) {
            String key = entry.getKey();
            ECSNode node = entry.getValue();
            String path = String.format("%s/%s", _rootZnode, node.getNodeName());
            if (path.equals(serverName)) {
                try {
                    if (_zooKeeper.exists(path, false) != null) {
                        byte[] data = NodeEvent.SHUTDOWN.name().getBytes();
                        _zooKeeper.setData(path, data, _zooKeeper.exists(path, false).getVersion());
                    }
                    logger.info("Removed " + serverName);
                    active_servers.remove(key);

                    updateMetadata();

                    return true;
                } catch (Exception e) {
                    logger.error("Error while removing node!");

                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    e.printStackTrace(pw);
                    logger.error(sw.toString());

                    return false;
                }
            }
        }

        logger.error("Could not find the node for removal");
        return false;
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

                    if (shutdown) {
                        completeShutdown();
                    }
                } else {
                    byte[] recvData = _zooKeeper.getData(path,
                            false, null);
                    NodeEvent status = NodeEvent.valueOf(new String(recvData,
                            "UTF-8").split("~")[0]);

                    if (status == NodeEvent.BOOT || status == NodeEvent.BOOT_COMPLETE) {
                        // Node was created
                        logger.info("New node created at: " + path);
                        // Subscribe to it
                        _zooKeeper.exists(path,
                                true);
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

    private String[] moveData(String path) {
        String serverName = path.substring(path.lastIndexOf("/") + 1);

        for (Map.Entry<String, ECSNode> entry : active_servers.entrySet()) {
            String key = entry.getKey();
            ECSNode node = entry.getValue();

            if (node.getNodeName().equals(serverName)) {
                Map.Entry<String, ECSNode> successor = active_servers.higherEntry(key);
                // If it wraps around:
                if (successor == null) {
                    successor = active_servers.firstEntry();
                }
                ECSNode successorVal = successor.getValue();
                if (successorVal.getNodeName().equals(serverName)) {
                    return null;
                }
                String znode = String.format("%s/%s", _rootZnode, successorVal.getNodeName());
                String lower = active_servers.lowerKey(key);
                if (lower == null) {
                    lower = active_servers.lastKey();
                }
                return new String[] { znode, lower, key, node.getNodeName() };
            }
        }

        return null;
    }

    public void sendMetadataBooted() {
        try {
            for (IECSNode node : active_servers.values()) {
                String path = String.format("%s/%s", _rootZnode, node.getNodeName());
                byte[] recvData = _zooKeeper.getData(path,
                        false, null);
                NodeEvent status = NodeEvent.valueOf(new String(recvData,
                        "UTF-8").split("~")[0]);

                if (status == NodeEvent.BOOT_COMPLETE) {
                    sendMetadata(path);
                }
            }
        } catch (Exception e) {
            logger.error("Error while getting data from ZooKeeper");
            logger.error(e.getMessage());
        }
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
            logger.error(e.getMessage());
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECS CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("addNodes <numberOfNodes>");
        sb.append(
                "\t\t randomly choose <numberOfNodes> servers from the available machines and start the KVServer by issuing an SSH call to the respective machine. This call launches the storage server. For simplicity, locate the KVServer.jar in the same directory as the ECS. All storage servers are initialized with the metadata and any persisted data, and remain in state stopped.\n");
        sb.append(PROMPT).append("start");
        sb.append(
                "\t\t starts the storage service by calling start() on all KVServer instances that participate in the service.\n");
        sb.append(PROMPT).append("stop");
        sb.append(
                "\t\t stops the service; all participating KVServers are stopped for processing client requests but the processes remain running.\n");
        sb.append(PROMPT).append("shutDown");
        sb.append("\t\t stops all server instances and exits the remote processes.\n");
        sb.append(PROMPT).append("addNode");
        sb.append(
                "\t\t create a new KVServer and add it to the storage service at an arbitrary position.\n");
        sb.append(PROMPT).append("removeNode <index of server>");
        sb.append("\t\t remove a server from the storage service at an arbitrary position.\n");

        sb.append(PROMPT).append("logLevel <level>");

        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("help");
        sb.append("\t\t shows this help menu\n");

        System.out.println(sb.toString());
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }
}
