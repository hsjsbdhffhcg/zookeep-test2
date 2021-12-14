package com.example.zookeepr;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;





    class ZooKeeperTest {
        private static final String SERVER_HOST = "127.0.0.1:2181";
        private static final int SESSION_TIME_OUT = 2000;
        private static ZooKeeper zooKeeper;

        @BeforeAll //这个注解必须注解static方法，在所有测试前运行一次
        static void setUp() {
            try {
                zooKeeper = new ZooKeeper(SERVER_HOST, SESSION_TIME_OUT, (watchedEvent) -> {});
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @AfterAll //这个注解必须注解static方法，在所有测试后运行一次
        static void tearDown() {
            if (zooKeeper != null) {
                try {
                    zooKeeper.close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        @Test
        void getAcl() {
            String path = "/abc";
            Stat stat = new Stat();
            zooKeeper.addAuthInfo("digest","wang:wang".getBytes());
            try {
                List<ACL> aclList = zooKeeper.getACL(path, stat);
                //zookeeper客户端中有很多方法都会将stat的值进行填充
                System.out.println(stat);
                for (ACL acl : aclList) {
                    System.out.println(acl.getPerms() + ": " + acl.getId());
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
//        public interface Perms {
//            int READ = 1 << 0; 1
//            int WRITE = 1 << 1; 2
//            int CREATE = 1 << 2; 4
//            int DELETE = 1 << 3; 8
//            int ADMIN = 1 << 4; 16
//            int ALL = READ | WRITE | CREATE | DELETE | ADMIN; 31
//        }
        }

        @Test
        void getDigestAcl() {
            String path = "/";
            Stat stat = new Stat();
            try {
                List<ACL> aclList = zooKeeper.getACL(path, stat);
                //zookeeper客户端中有很多方法都会将stat的值进行填充
                System.out.println(stat);
                for (ACL acl : aclList) {
                    System.out.println(acl.getPerms() + ": " + acl.getId());
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Test
        void delete() {
            String path = "/watcher_test1";
            try {
                Stat stat = zooKeeper.exists(path, null);
                if(stat != null)
                    zooKeeper.delete(path, -1);
                else
                    System.out.println("节点不存在");
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Test
        void setData() {
            String path = "/abc";
            byte[] data = ("my_data_" + LocalDateTime.now()).getBytes();
            try {
                Stat stat = zooKeeper.exists(path, null);
                System.out.println(stat.toString());
                int version = stat.getVersion();
                zooKeeper.setData(path, data, version);
                //version如果取值为-1则忽略版本号
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Test
        void getData() {
            String path = "/watcher_test";
            try {
                byte[] data = zooKeeper.getData("/watcher_test", null, null);
                System.out.println(new String(data));
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Test
        void ls() {
            List<String> zooChildren = null;
            try {
                zooChildren = zooKeeper.getChildren("/", false);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Znodes of /:");
            for (String zooChild : Objects.requireNonNull(zooChildren))
                System.out.println(zooChild);
        }

        @Test
        void acl() {
            zooKeeper.addAuthInfo("digest", "peter:820517".getBytes());
//        List<ACL> acl = new ArrayList<>();
//        acl.add(new ACL(31, new Id("world", "anyone")));
//        acl.add(new ACL(31, new Id("digest", "peter:Sg4hce3fFia9OEm849qILF+EkxI=")));
            try {
                zooKeeper.create("/acl_test", "data".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
                System.out.println(new String(zooKeeper.getData("/acl_test", false, null)));

                ZooKeeper zkTemp = new ZooKeeper(SERVER_HOST, SESSION_TIME_OUT, (watchedEvent) -> {
                });
                System.out.println(new String(zkTemp.getData("/acl_test", false, null)));
                //此处会抛出异常
            } catch (KeeperException | InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }

        @Test
        void watcher() {
            String path = "/watcher_test";
            byte[] data = "data".getBytes();
            class MyWatch implements Watcher, Runnable {
                @Override
                public void run() {
                    try {
                        synchronized (this) {
                            while (true)
                                wait();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        Thread.currentThread().interrupt();
                    }
                }

                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDataChanged) {
                        try {
                            System.out.printf("data form /watch_test is: %s",
                                    new String(zooKeeper.getData(path, this, null)));
                        } catch (KeeperException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            try {
                zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                MyWatch myWatch = new MyWatch();
                myWatch.run();
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

