package org.lemon.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 基本的crud操作, 熟悉zk的基本使用
 */
public class ZookeeperDemo {
    ZooKeeper zooKeeper;

    @Before
    public void init() throws Exception {
        String conn = "192.168.142.133:2181";
        zooKeeper = new ZooKeeper(conn, 4000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getPath());
                System.out.println(event);
//                zooKeeper.register(this); 无效
            }
        });
    }

    @Test
    public void getData() throws Exception {
        Stat stat = new Stat();//获取节点的详细信息
        byte[] data = zooKeeper.getData("/test", false, stat);
        System.out.println(new String(data));
        System.out.println(stat);
    }

    @Test
    public void getData2() throws Exception {
        // 注册监听，但是这里的注册只能消费一次监听事件，
        byte[] data = zooKeeper.getData("/test", true, null);
        System.out.println(new String(data));
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void getData3() throws Exception {
        Stat stat = new Stat();

        //这里注册了监听事件，默认情况下注册一次只能监听一次，
        //这里在每次执行监听动作的时候再手动监听一次，就可以实现持续监听的功能
        zooKeeper.getData("/test", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    zooKeeper.getData(event.getPath(), this, null);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println(event.getPath());
            }
        }, stat);
        System.out.println(stat);
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void getChild() throws Exception {
        List<String> children = zooKeeper.getChildren("/test", false);
        children.stream().forEach(System.out::println);
    }

    @Test
    public void createData() throws Exception {
        //设置权限列表
        List<ACL> list = new ArrayList<>();
        int perm = ZooDefs.Perms.ADMIN | ZooDefs.Perms.READ;//cdwra
        ACL acl = new ACL(perm, new Id("world", "anyone"));
        ACL acl2 = new ACL(perm, new Id("ip", "192.168.142.133"));
        ACL acl3 = new ACL(perm, new Id("ip", "127.0.0.1"));
        list.add(acl);
        list.add(acl2);
        list.add(acl3);
        zooKeeper.create("/test", "hello".getBytes(), list, CreateMode.PERSISTENT);
    }

    @Test
    public void getChild2() throws Exception {
        List<String> children = zooKeeper.getChildren("/test", event -> {
            System.out.println(event.getPath());
            try {
                zooKeeper.getChildren(event.getPath(), false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        children.stream().forEach(System.out::println);
        Thread.sleep(Long.MAX_VALUE);
    }
}
