package c02;

import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Set;

public class CacheRowsThread extends Thread {
    private Jedis conn;
    private boolean quit;
    public CacheRowsThread() {
        this.conn = new Jedis("localhost");
        this.conn.select(10);
    }

    public void quit() {
        quit = true;
    }

    public void run() {
        Gson gson = new Gson();
        while (!quit) {
            Set<Tuple> range = conn.zrangeWithScores("schedule:", 0, 0);
            Tuple next = range.size() > 0 ? range.iterator().next() : null;
//            System.out.println(gson.toJson(range));

            long now = System.currentTimeMillis() / 1000;
            if (next == null || next.getScore() > now) {
                try {
                    sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                continue;
            }
            String rowId = next.getElement();
            double delay = conn.zscore("delay:", rowId);
            if (delay <= 0) {
                conn.zrem("schedule:", rowId);
                conn.zrem("delay:", rowId);

                continue;
            }
            Inventory row = Inventory.get(rowId);
            conn.set("inv:" + rowId, gson.toJson(row));
            conn.zadd("schedule:", now + delay, rowId);
        }
    }
}
