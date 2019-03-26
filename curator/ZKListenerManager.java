import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 此类用于对zookeeper的节点添加监听, 包含单节点监听、子节点目录监听、树形目录监听
 * @version 2018/07/20
 */
public class ZKListenerManager {
    public static final Logger logger = LoggerFactory.getLogger(ZKListenerManager.class);
    /**
     * 添加节点监听
     * @param connectString zookeeper的连接地址
     * @param listenerPath 待监听的zookeeper节点路径
     * @param nodeCacheListener 单节点类型的监听器
     * @return nodeCache 返回该类型的对象，以用于可能存在的撤销监听的操作
     * @throws Exception
     */
    public static NodeCache addNodeCacheListener(String connectString, String listenerPath, NodeCacheListener 
            nodeCacheListener) throws Exception {
        CuratorFramework curatorClient = CuratorManager.getClient(connectString);
        
        curatorClient.start();
        NodeCache nodeCache = new NodeCache(curatorClient, listenerPath);
        // 构造方法：第三个参数可以省略（数据压缩参数）: NodeCache​(CuratorFramework client, String path, boolean dataIsCompressed)
        
        // 如果为true则首次不会缓存节点内容到cache中，默认为false,设置为true首次不会触发监听事件（建议用true）
        nodeCache.start(true);
       
        nodeCache.getListenable().addListener(nodeCacheListener);
        return nodeCache;
    }
    
    /**
     * 添加子节点监听
     * @param connectString zookeeper的连接地址
     * @param listenerPath 待监听的zookeeper节点路径
     * @param pathChildrenCacheListener 子节点目录类型的监听器
     * @return pathChildrenCache 返回该类型的对象，以用于可能存在的撤销监听的操作
     * @throws Exception 
     */
    public static PathChildrenCache addPathCacheListener(String connectString, String listenerPath, PathChildrenCacheListener 
            pathChildrenCacheListener) throws Exception {
        CuratorFramework curatorClient = CuratorManager.getClient(connectString);
        curatorClient.start();
        PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorClient, listenerPath, true);
        // 第三个参数：cacheData - 如果为 true, 则除了统计属性之外, 还缓存节点内容（缓存的数据可以用于获取出来输出），其它可构造参数添加见文档
        
        // 三种启动模式
        pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        // pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);
        // pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        /*
         * start()可以传参数，如下：
         * NORMAL: 初始时为空(默认)
         * BUILD_INITIAL_CACHE: 在这个方法返回之前调用rebuild()。
         * POST_INITIALIZED_EVENT: 当Cache初始化数据后发送一个PathChildrenCacheEvent.Type#INITIALIZED事件
         */
        
        pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
        return pathChildrenCache;
    }
    
    /**
     * 添加树形目录节点监听
     * @param connectString zookeeper的连接地址
     * @param listenerPath 待监听的zookeeper节点路径
     * @param treeCacheListener 树形目录类型的监听器
     * @return treeCache 返回该类型的对象，以用于可能存在的撤销监听的操作
     * @throws Exception 
     */
    public static TreeCache addTreeCacheListener(String connectString, String listenerPath, TreeCacheListener 
            treeCacheListener) throws Exception {
        CuratorFramework curatorClient = CuratorManager.getClient(connectString);
        curatorClient.start();
        TreeCache treeCache = new TreeCache(curatorClient, listenerPath);
        // 没有开启模式作为入参的方法
        treeCache.start();
        
        treeCache.getListenable().addListener(treeCacheListener);
        return treeCache;
    }
    
    /**
     * 关闭节点监听
     * @param connectString
     * @param listenerPath
     * @param nodeCache
     * @param nodeCacheListener
     * @throws Exception
     */
    public static void deleteNodeCacheListener(String connectString, String listenerPath, NodeCache 
            nodeCache, NodeCacheListener nodeCacheListener) throws Exception {
        nodeCache.getListenable().removeListener(nodeCacheListener);
        nodeCache.close();
        
    }
    
    
    /**
     * 关闭子节点监听
     * @param connectString
     * @param listenerPath
     * @param pathChildrenCache
     * @param pathChildrenCacheListener
     * @throws Exception
     */
    public static void deletePathChildrenCacheListener(String connectString, String listenerPath, PathChildrenCache
            pathChildrenCache, PathChildrenCacheListener pathChildrenCacheListener) throws Exception {
        pathChildrenCache.getListenable().removeListener(pathChildrenCacheListener);
        pathChildrenCache.close();
        
    }
    
    /**
     * 关闭树形目录节点监听
     * @param connectString
     * @param listenerPath
     * @param treeCache
     * @param treeCacheListener
     * @throws Exception
     */
    public static void deleteTreeCacheListener(String connectString, String listenerPath, TreeCache treeCache, TreeCacheListener 
            treeCacheListener) throws Exception {
        treeCache.getListenable().removeListener(treeCacheListener);
        treeCache.close();
    }
}
