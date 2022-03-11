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
                            false, null);
                    String recv = new String(dataBytes,
                            "UTF-8");
                    logger.info("ZooKeeper Notification:" + recv);
                    String[] data = recv.split("~");

                    switch (NodeEvent.valueOf(data[0])) {
                        case METADATA:
                            caller.initKVServer(data[1]);
                            break;
                        case START:
                            caller.start();
                            break;
                        case STOP:
                            caller.stop();
                            break;
                        case SHUTDOWN:
                            caller.shutDown();
                            if (data.length == 1) {
                                caller.completeShutdown();
                                break;
                            }
                            // NOTE: This fallthrough is deliberate
                        case COPY:
                            String[] moveData = data[1].split(",");
                            String[] range = { moveData[0], moveData[1] };
                            caller.moveData(range, moveData[2]);
                            break;
                        case MOVE:
                            caller.completeMove();
                            break;
                        // Ignored events:
                        case BOOT:
                        case METADATA_COMPLETE:
                        case COPY_COMPLETE:
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
        }

        try {
            String watchPath = String.format("%s/%s", caller._rootZnode, caller.name);
            logger.info("Resetting watchers on " + watchPath + " ...");
            // Since notifications are a one time thing, we must reset the watcher
            caller._zooKeeper.getData(watchPath, this, null);
        } catch (Exception e) {
            logger.error("Error while resetting watcher!");
            logger.error(e.getMessage());
        }
    }

}
