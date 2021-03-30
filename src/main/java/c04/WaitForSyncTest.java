package c04;

import redis.clients.jedis.Jedis;

import java.util.UUID;

public class WaitForSyncTest {
    public static void main(String[] args) throws Exception {
        testWaitForSync();
    }

    public static void testWaitForSync() throws Exception {
        Jedis mconn = new Jedis("localhost", 6379);
        Jedis sconn = new Jedis("localhost", 6380);
        waitForSync(mconn, sconn);
    }

    public static void waitForSync(Jedis mconn, Jedis sconn) throws Exception {
        String identifier = UUID.randomUUID().toString();
        mconn.zadd("sync:wait", System.currentTimeMillis(), identifier);

        while (!sconn.info().contains("master_link_status:up")) {
            Thread.sleep(1);
        }

        while (sconn.zscore("sync:wait", identifier) == null) {
            Thread.sleep(1);
        }

        long deadline = System.currentTimeMillis()  + 1010;
        while (System.currentTimeMillis() < deadline) {
            // todo 一直没有出现,是否以这种方式检查是否存储到磁盘存疑
            if (sconn.info().contains("aof_pending_bio_fsync:0"))
                break;
            Thread.sleep(1);
        }
        mconn.zrem("sync:wait", identifier);
        mconn.zremrangeByScore("sync:wait", 0, System.currentTimeMillis() - 900 * 1000);
    }
}
