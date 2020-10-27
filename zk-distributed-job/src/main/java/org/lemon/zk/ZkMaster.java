package org.lemon.zk;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.stream.Collectors;

public class ZkMaster {
    private static final String ROOTPATH = "/longmeng";
    private static final String SERVICE = ROOTPATH + "/service";
    private static String node;//表示当前节点
    private boolean master = false;
    private String zkServer = "192.168.142.133:2181";
    private ZkClient zkClient;
    private static ZkMaster zkMaster;
    private static final String NODE_SLAVE = "slave";
    private static final String NODE_MASTER = "master";

    private ZkMaster() {
        //初始化zk对象
        zkClient = new ZkClient(zkServer, 2000, 5000);
        //创建rootPath节点
        buildRootNode();
        //创建当前节点,并且添加监听事件
        createServiceNode();
    }

    //创建根节点
    private void buildRootNode() {
        if (!zkClient.exists(ROOTPATH)) {
            zkClient.createPersistent(ROOTPATH);
            System.out.println("init root node");
        }
    }

    //创建服务节点,
    private void createServiceNode() {
        //判断当前节点是否存在
        if (StringUtils.isBlank(node)) {
            //新增节点
            node = zkClient.createEphemeralSequential(SERVICE, NODE_SLAVE);
            //触发选举
            doElection();
            //添加监听事件
            zkClient.subscribeChildChanges(ROOTPATH, (parentPath, currentChilds) -> {
                doElection();
            });
        }
    }

    //选举
    public void doElection() {
        //拿到所有的节点，判断第一个节点是否为master,是就不做处理,  否则将第一个node设置为master
        boolean existMaster = zkClient.getChildren(ROOTPATH).stream()
                .map(p -> ROOTPATH + "/" + p)
                .map(p -> zkClient.readData(p))
                .anyMatch(d -> "master".equals(d));
        if (!existMaster) {
            Map<String, Object> childData = zkClient.getChildren(ROOTPATH)
                    .stream()
                    .map(p -> ROOTPATH + "/" + p)
                    .collect(Collectors.toMap(p -> p, p -> zkClient.readData(p)));

            if (childData.containsValue(NODE_MASTER)) {
                return;
            }
            childData.keySet().stream().sorted().findFirst().ifPresent(p -> {
                if (p.equals(node)) { // 设置最小值序号为master 节点
                    zkClient.writeData(node, NODE_MASTER);
                    master = true;
                    System.out.println("当前当选master" + node);
                }
            });
        }
        //遍历判断是否为当前节点, 是将当前节点设置为master
    }

    public static ZkMaster getInstance() {
        if (zkMaster == null) {
            zkMaster = new ZkMaster();
        }
        return zkMaster;
    }

    public boolean isMaster() {
        return zkMaster.master;
    }

    public static void main(String[] args) throws Exception {
//        ZkClient client = new ZkClient("192.168.142.133:2181", 2000, 5000);
        //看创建临时序号节点的返回值
        //看获取子节点列表的返回值是什么

//        String nodePath = client.createEphemeralSequential("/longmeng/myzk", "master");
//        System.out.println(nodePath);
//        String nodePath2 = client.createEphemeralSequential("/longmeng/myzk", "slave");
//        System.out.println(nodePath2);
//        List<String> list = client.getChildren("/longmeng");
//        list.forEach(System.out::println);

//        String result = client.readData(nodePath);
//        System.out.println(result);
        System.out.println(ZkMaster.getInstance().isMaster());
    }
}
