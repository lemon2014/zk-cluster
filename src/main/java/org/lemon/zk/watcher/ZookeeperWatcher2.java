package org.lemon.zk.watcher;

import org.apache.zookeeper.*;

public class ZookeeperWatcher2 {
    private static String connectString = "192.168.142.128:2181,192.168.142.129:2181,192.168.142.130:2181";
    private static int sessionTimeout = 5000;

    public static void main(String[] args) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("===============abc================");
            }
        });
        if (null != zooKeeper.exists("/longmeng", false)) {
            zooKeeper.delete("/longmeng", -1);
        }
        zooKeeper.create("/longmeng", "abc".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }
}
