package app_kvServer;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import org.apache.log4j.Logger;

import app_kvServer.IKVServer.Status;
import ecs.IECSNode.NodeEvent;
import logger.LogSetup;

public class ZooKeeperWatcher implements Watcher {

    private static Logger logger = Logger.getRootLogger();

    private KVServer caller = null;

    public ZooKeeperWatcher(KVServer caller) {
        this.caller = caller;
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("WATCHER NOTIFICATION (from storage server)!");
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
                if (keeperState == KeeperState.Closed) {
                    return;
                } else {
                    logger.info("Successfully connected to ZK server!");
                }
                break;
            case NodeDataChanged:
                try {
                    byte[] dataBytes = caller._zooKeeper.getData(path,
                            this, null);
                    String[] data = new String(dataBytes,
                            "UTF-8").split("~");
                    logger.info("Got:" + String.join("", data));

                    switch (NodeEvent.valueOf(data[0])) {
                        case METADATA:
                            caller.loadMetadata(data[1]);
                            break;
                        case START:
                            caller.setStatus(Status.STARTED);
                            return;
                        case SHUTDOWN:
                            caller.shutdown();
                            return;
                        case MOVE:
                            String[] moveData = data[1].split(",");
                            String[] range = { moveData[0], moveData[1] };
                            caller.moveData(range, moveData[2]);
                            return;
                        // Ignored events:
                        case BOOT:
                        case BOOT_COMPLETE:
                        case METADATA_COMPLETE:
                        case MOVE_COMPLETE:
                            break;
                        default:
                            logger.error("Unrecognized node event:" + data[0]);
                    }
                } catch (Exception e) {
                    logger.error("Error while getting data");
                    logger.error(e.getMessage());
                }
                break;
            case NodeDeleted:
                logger.info("node " + path + " Deleted");
                return;
        }

        try {
            logger.info("Resetting watchers ...");
            // Since notifications are a one time thing, we must reset the watcher
            // Subscribe to both the parent and the node itself
            caller._zooKeeper.exists(caller._rootZnode, true);
            caller._zooKeeper.exists(String.format("%s/%s", caller._rootZnode, caller.name), true);
        } catch (Exception e) {
            logger.error("Error while resetting watcher!");
            logger.error(e.getMessage());
        }
    }

}
