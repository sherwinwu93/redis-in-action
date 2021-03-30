package c04;

import redis.clients.jedis.Jedis;

import java.util.UUID;

public class C04 {
    public static void main(String[] args) throws Exception {
    }

    public void testProcessLogs() throws Exception {

    }
    

    public static void testWaitForSync() throws Exception {
        Jedis mconn = new Jedis("localhost", 6379);
        Jedis sconn = new Jedis("localhost", 6380);
        waitForSync(mconn, sconn);
    }

    public static void waitForSync(Jedis mconn, Jedis sconn) throws Exception {
        String identifier = UUID.randomUUID().toString();
        mconn.zadd("sync:wait", System.currentTimeMillis(), identifier);

        while (!sconn.info().contains("aof_pending_bio_fsync")) {
            Thread.sleep(10);
        }

        while (sconn.zscore("sync:wait", identifier) == null) {
            Thread.sleep(10);
        }

    }
}
