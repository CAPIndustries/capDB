package app_kvECS;

import java.util.List;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.*;

import org.apache.log4j.Logger;

import ecs.IECSNode.NodeEvent;

import app_kvECS.ECSClient;

import logger.LogSetup;

public class ZooKeeperWatcher implements Watcher {

    private static Logger logger = Logger.getRootLogger();

    private ECSClient caller = null;

    public ZooKeeperWatcher(ECSClient caller) {
        this.caller = caller;
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("WATCHER NOTIFICATION!");
        if (event == null) {
            return;
        }

        // Get connection status
        KeeperState keeperState = event.getState();
        // Event type
        EventType eventType = event.getType();
        // Affected path
        String path = event.getPath();

        logger.info("Connection status:\t" + keeperState.toString());
        logger.info("Event type:\t" + eventType.toString());

        switch (eventType) {
            case None:
                logger.info("Successfully connected to ZK server!");
                break;
            case NodeDataChanged:
                try {
                    logger.info("Node data update");
                    byte[] dataBytes = caller._zooKeeper.getData(path,
                            false, null);
                    String[] data = new String(dataBytes,
                            "UTF-8").split("~");
                    logger.info("Got:" + data);
                    switch (NodeEvent.valueOf(data[0])) {
                        case BOOT_COMPLETE:
                            // Send metadata
                            logger.info("Boot complete!");
                            break;
                        // Skip the following events:
                        case BOOT:
                            break;
                        default:
                            logger.error("Unrecognized node event:" + data[0]);
                    }
                } catch (Exception e) {
                    logger.error("Error while getting data");
                    logger.error(e.getMessage());
                }
                break;
            case NodeChildrenChanged:
                logger.info("Child node change");
                // TODO: Recalc metadata and send to all nodes
                break;
            case NodeCreated:
                // TODO: Delete if not using later...
                logger.info("Node creation");
                break;
            case NodeDeleted:
                logger.info("node " + path + " Deleted");
                break;
        }

        // Subscribe once more
        try {
            caller._zooKeeper.exists(caller._rootZnode, true);
            String test = caller._rootZnode;
            List<String> servers = caller._zooKeeper.getChildren(caller._rootZnode,
                    true);
            for (String server : servers) {
                String p = String.format("%s/%s", caller._rootZnode, server);
                caller._zooKeeper.exists(p, true);
            }
        } catch (Exception e) {
            logger.error("Error while resetting watcher!");
            logger.error(e.getMessage());
        }
    }

}
