package org.lemon.zk.znode;

import org.apache.zookeeper.*;

public class ZookeeperCrud {

    private ZooKeeper zooKeeper;

    private String connectString = "192.168.142.128:2181,192.168.142.129:2181,192.168.142.130:2181";
    private int sessionTimeout = 5000;
    private Watcher watcher = null;

    public ZookeeperCrud() throws Exception {
        zooKeeper = new ZooKeeper(connectString, sessionTimeout, watcher);
    }

    /**
     * 这里重点是要注意下CreateMode的四种模式
     * PERSISTENT
     * PERSISTENT_SEQUENTIAL
     * EPHEMERAL
     * EPHEMERAL_SEQUENTIAL
     */
    public String createPersistent(String path, String data) {
        String result = null;
        try {
            result = zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        ZookeeperCrud crud = new ZookeeperCrud();
        String str = crud.createPersistent("/test", "123123");
        System.out.println(str);
    }
}
