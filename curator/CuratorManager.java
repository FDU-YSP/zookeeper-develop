import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
/**
 * 该类用于建立一个zookeeper的连接对象(客户端)
 * @version 2018/07/23
 */
public class CuratorManager {
    
    public static CuratorFramework getClient(String connectString) {
        // 连接地址以后应当从配置对象中获取
        
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        // 重试策略：刚开始的重试事件是1000，后面一直增加，最多不超过三次, 此处的第二个参数应当源自xml解析类对象的值
        
        CuratorFramework curatorClient = null;
        curatorClient = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        // build():应用这些值去构建对象
        
        return curatorClient;
        
    }
}
