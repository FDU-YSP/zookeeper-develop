import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.efounder.config.plugin.ConfigUploadPlugin;
/**
 * 该类包含对zookeeper的节点存在检查、创建节点、删除节点、强制删除节点、更新节点操作
 * @version 2018/07/19
 */
public class ZKNodeManager {
    public static final Logger logger = LoggerFactory.getLogger(ZKNodeManager.class);
    /**
     * 检查路径节点是否存在
     * @param connectString zookeeper的连接地址
     * @param operatorAddress 待检查的zookeepre节点路径
     * @return boolean(true:存在; false:不存在)
     * @throws Exception
     */
    public static boolean checkNodeExist(String connectString, String operatorAddress) {
        
        Stat nodeExist = null;
        CuratorFramework curatorClient = CuratorManager.getClient(connectString);
        try {
            curatorClient.start();
            // 检查节点是否存在，该方法返回Stat实例，不存在则返回null
            nodeExist = curatorClient.checkExists().forPath(operatorAddress);
        } catch (Exception e) {
            logger.error("检查节点是否存在时发生异常", e);
        } finally {
            curatorClient.close();
        }
        if(nodeExist == null) {
            return false;
        } else {
            return true;
        }
    }
    /**
     * 创建节点
     * @param connectString zookeeper的连接地址
     * @param operatorAddress 待创建的zookeepre节点路径
     * @param nodeData 待创建的节点数据
     * @throws Exception
     */
    public static void createNode(String connectString, String operatorAddress, byte[] nodeData) {

        CuratorFramework curatorClient = CuratorManager.getClient(connectString);
        // 此方法默认创建持久节点
        // curatorClient.create().withMode(CreateMode.PERSISTENT).forPath(operatorAddress, nodeData);
        try {
            curatorClient.start();
            curatorClient.create().creatingParentsIfNeeded().forPath(operatorAddress, nodeData);
        } catch (Exception e) {
            logger.error("创建节点时发生异常", e);
        } finally {
            curatorClient.close();
        }
        // curatorClient.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT)
        //        .forPath(operatorAddress, nodeData);
    }
    /**
     * 强制删除节点方法，如果当前节点有子节点，将会递归全部删除
     * @param connectString zookeeper的连接地址
     * @param operatorAddress 待删除的zookeepre节点路径
     * @throws Exception
     */
    public static void forceDeleteNode(String connectString, String operatorAddress) {
        
        CuratorFramework curatorClient = CuratorManager.getClient(connectString);
        try {
            curatorClient.start();
            curatorClient.delete().deletingChildrenIfNeeded().forPath(operatorAddress);
        } catch (Exception e) {
            logger.error("强制删除节点时发生异常", e);
        } finally {
            curatorClient.close();
        }
    }
    /**
     * 普通删除方法，只会尝试删除节点本身，如果存在子节点，删除将会抛出异常
     * @param connectString zookeeper的连接地址
     * @param operatorAddress 待删除的zookeepre节点路径
     * @throws Exception
     */
    public static void deleteNode(String connectString, String operatorAddress) {
        
        CuratorFramework curatorClient = CuratorManager.getClient(connectString);
        try {
            curatorClient.start();
            curatorClient.delete().forPath(operatorAddress);
        } catch (Exception e) {
            logger.error("删除节点时发生异常", e);
        } finally {
            curatorClient.close();   
        }
    }
    /**
     * 节点修改方法
     * @param connectString zookeeper的连接地址
     * @param operatorAddress 待更新的zookeepre节点路径
     * @param nodeData 待更新的zookeepre节点数据
     * @return
     * @throws Exception
     */
    public static Stat updataNode(String connectString, String operatorAddress, byte[] nodeData) {
        
        CuratorFramework curatorClient = CuratorManager.getClient(connectString);
        Stat stat = new Stat();
        try {
            curatorClient.start();
            curatorClient.setData().forPath(operatorAddress, nodeData);
        } catch (Exception e) {
            logger.error("更新节点时发生异常", e);
        } finally {
            curatorClient.close();
        }
        return stat;
    }
}
