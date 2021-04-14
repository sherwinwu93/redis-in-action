package c04;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class TokenTest {
    public static void main(String[] args) {

    }

    public static void updateTokenPipeline(Jedis conn, String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        // 使用流水线减少后台与redis通信次数
        Pipeline pipe = conn.pipelined();
        pipe.multi();
        pipe.hset("login:", token, user);
        pipe.zadd("recent:", timestamp, token);
        if (item != null) {
            pipe.zincrby("viewed:", -1, item);
            pipe.zadd("viewed:" + token, timestamp, item);
            pipe.zremrangeByRank("viewed:" + token, 0, -26);
        }
        pipe.exec();
    }
}
