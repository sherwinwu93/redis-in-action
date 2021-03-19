package c02;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CleanFullSessionsThread extends Thread {
    private Jedis conn;
    private int limit;
    private boolean quit;

    public CleanFullSessionsThread(int limit) {
        this.conn = new Jedis("localhost");
        this.conn.select(10);
        this.limit = limit;
    }

    public void quit() {
        quit = true;
    }

    public void run() {
        while (!quit) {
            long recentSize = conn.zcard("recent:");
            if (recentSize <= limit) {
                try {
                    sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }

            long endIndex = Math.min(recentSize - limit, 100);
            Set<String> sessionSet = conn.zrange("recent:", 0, endIndex - 1);
            String[] sessions = sessionSet.toArray(new String[sessionSet.size()]);

            List<String> sessionViewed$CartKeys = new ArrayList<>();
            for (String session : sessions) {
                sessionViewed$CartKeys.add("viewed:" + session);
                sessionViewed$CartKeys.add("cart:" + session);
            }

            conn.zrem("recent:", sessions);
            conn.hdel("login:", sessions);
            conn.del(sessionViewed$CartKeys.toArray(new String[sessionViewed$CartKeys.size()]));
        }
    }
}
