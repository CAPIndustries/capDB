package app_kvECS;

import java.util.List;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.*;

import org.apache.log4j.Logger;

import ecs.IECSNode.NodeEvent;

import app_kvECS.ECS;

import logger.LogSetup;

public class ZooKeeperWatcher implements Watcher {

    private static Logger logger = Logger.getRootLogger();

    private ECS caller = null;

    public ZooKeeperWatcher(ECS caller) {
        this.caller = caller;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event == null) {
            return;
        }

        // Get connection status
        KeeperState keeperState = event.getState();
        // Event type
        EventType eventType = event.getType();
        // Affected path
        String path = event.getPath();

        logger.info("Event from:\t" + path);
        logger.info("Connection status:\t" + keeperState.toString());
        logger.info("Event type:\t" + eventType.toString());

        switch (eventType) {
            case None:
                logger.info("Successfully connected to ZK server!");
                break;
            case NodeDataChanged:
                try {
                    logger.info("Node data update");
                    // Resubscribe below:
                    byte[] dataBytes = caller._zooKeeper.getData(path,
                            true, null);
                    String recv = new String(dataBytes,
                            "UTF-8");
                    logger.info("ZooKeeper Notification:" + recv);
                    String[] data = recv.split("~");

                    switch (NodeEvent.valueOf(data[0])) {
                        case METADATA_COMPLETE:
                            logger.info("Metadata ACK!");
                            break;
                        case MOVE_COMPLETE:
                            caller.sendMetadata();
                            break;
                        case COPY_COMPLETE:
                            caller.completeCopy(path);
                            break;
                        // Skip the following events:
                        case START:
                        case BOOT:
                        case METADATA:
                        case STOP:
                        case SHUTDOWN:
                        case COPY:
                        case MOVE:
                            break;
                        default:
                            logger.error("Unrecognized node event:" + data[0]);
                    }

                    // Resubscribe back:
                    // logger.info("Resubscribing back to " + path);
                    // caller._zooKeeper.exists(path, true);
                } catch (Exception e) {
                    logger.error("Error while getting data");
                    logger.error(e.getMessage());
                }
                break;
            case NodeChildrenChanged:
                // Is it a new child? Or did a node get deleted?
                logger.info("Node created/deleted");
                caller.nodeRemovedCreated();

                // Resubscribe back:
                try {
                    caller._zooKeeper.getChildren(caller._rootZnode,
                            true);
                } catch (Exception e) {
                    logger.error("Error while resubscribing back to obtain children");
                }
                break;
            default:
                break;
        }
    }

}
