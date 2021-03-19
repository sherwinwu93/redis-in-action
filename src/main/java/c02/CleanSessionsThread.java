package c02;

import com.google.gson.Gson;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CleanSessionsThread extends Thread {
    private Jedis conn;
    // 限制session数量(即login:)
    private int limit;
    private boolean quit;

    public CleanSessionsThread(int limit) {
        this.conn = new Jedis("localhost");
        this.conn.select(10);
        this.limit = limit;
    }

    public void quit() {
        quit = true;
    }

    public void run() {
        while (!quit) {
            long size = conn.zcard("recent:");
            System.out.println("CleanSessionsThread.run...size:" + size);
            if (size <= limit) {
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }

            long endIndex = Math.min(size - limit, 100);
            // zrange 和 zrangeByScore的区别
            Set<String> tokenSet = conn.zrange("recent:", 0, endIndex - 1);
            String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);

            List<String> viewedKeys = new ArrayList<>();
            for (String token : tokenSet) {
                viewedKeys.add("viewed:" + token);
            }
            Gson gson = new Gson();
            System.out.println(gson.toJson(tokens));
            conn.hdel("login:", tokens);
            conn.zrem("recent:", tokens);
            conn.del(viewedKeys.toArray(new String[viewedKeys.size()]));
        }
    }
}
