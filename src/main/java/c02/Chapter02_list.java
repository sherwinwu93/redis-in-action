import c02.CacheRowsThread;
import c02.Callback;
import c02.CleanFullSessionsThread;
import c02.CleanSessionsThread;
import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

public class Chapter02_list {
    public static void main(String[] args) throws InterruptedException {
        new Chapter02_list().run();
    }

    public void run() throws InterruptedException {
        Jedis conn = new Jedis("localhost");
        conn.select(10);

        testLoginCookies(conn);
    }

    public void testLoginCookies(Jedis conn) throws InterruptedException {
        System.out.println("\n--- testLoginCookies ---");
        String token = UUID.randomUUID().toString();

        updateToken(conn, token, "username", "itemX");
        System.out.println("We just logged-in/updated token: " + token);
        System.out.println("For user: 'username'");
        System.out.println();

        System.out.println("What username do we get when we look-up that token?");
        String r = checkToken(conn, token);
        System.out.println(r);
        System.out.println();
        assert r != null;

        System.out.println("Let's drop the maximum number of cookies to 0 to clean them out");
        System.out.println("We will start a thread to do the cleaning, while we stop it later");

        CleanSessionsThread thread = new CleanSessionsThread(1);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The clean sessions thread is still alive?!?");
        }

        long s = conn.hlen("login:");
        System.out.println("The current number of sessions still available is: " + s);
        assert s == 0;
    }

    public String checkToken(Jedis conn, String token) {
        return conn.hget("login:", token);
    }

    public void updateToken(Jedis conn, String token, String user, String item) {
        conn.hset("login:", token, user);

        long timestamp = System.currentTimeMillis() / 1000;
        conn.zadd("recent:", timestamp, token);

        if (item != null) {
            conn.rpush("viewed:" + token, item);
            remList(conn, "viewed:" + token, 26);
            // 商品总浏览数
            conn.zincrby("viewed:", -1, item);
        }

    }

    private void remList(Jedis conn, String key, long limit) {
        String lindex = conn.lindex(key, limit);
        if (lindex == null) {
            return;
        }
        conn.lpop(key);
        remList(conn, key, limit);
    }

}
