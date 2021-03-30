import c02.CacheRowsThread;
import c02.Callback;
import c02.CleanFullSessionsThread;
import c02.CleanSessionsThread;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

public class Chapter02 {
    public static void main(String[] args) throws InterruptedException {
        new Chapter02().run();
    }

    public void run() throws InterruptedException {
        Jedis conn = new Jedis("localhost");
        conn.select(10);

        testLoginCookies(conn);
        testShoppingCartCookies(conn);
        testCacheRequest(conn);
        testCacheRows(conn);
    }

    public void testCacheRows(Jedis conn) throws InterruptedException {
        System.out.println("--- 测试cache rows---");
        System.out.println("首先,每5秒缓存一次itemX");
        scheduleRowCache(conn, "itemX", 5);
        System.out.println("我们的schedule:");
        Set<Tuple> s = conn.zrangeWithScores("schedule:", 0, -1);
        for (Tuple tuple : s) {
            System.out.println(" " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert s.size() != 0;

        System.out.println("开始缓存数据的线程");

        CacheRowsThread thread = new CacheRowsThread();
        thread.start();

        Thread.sleep(1000);
        System.out.println("已经缓存的数据:");
        String r = conn.get("inv:itemX");
        System.out.println(r);
        assert r != null;
        System.out.println();

        System.out.println("5s后再检查一次");
        Thread.sleep(5000);
        System.out.println("注意数据已经改变了,time已经增加了大致5s");
        String r2 = conn.get("inv:itemX");
        System.out.println(r2);
        System.out.println();
        assert r2 != null;
        assert !r.equals(r2);

        System.out.println("强制un-caching");
        scheduleRowCache(conn, "itemX", -1);
        Thread.sleep(1000);
        r = conn.get("inv:itemX");
        System.out.println("缓存清空了吗? " + (r == null));
        assert r == null;

        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive())  {
            throw new RuntimeException("缓存数据的线程仍然存在?");
        }
    }

    public void scheduleRowCache(Jedis conn, String rowId, int delay) {
        conn.zadd("delay:", delay, rowId);
        conn.zadd("schedule:", System.currentTimeMillis() / 1000, rowId);
    }

    public void testCacheRequest(Jedis conn) {
        System.out.println("--- 测试cache request--");
        String token = UUID.randomUUID().toString();

        Callback callback = new Callback() {
            public String call(String request) {
                return "content for " + request;
            }
        };

        updateToken(conn, token, "username", "itemX");
        String url = "http://test.com/?item=itemX";
        System.out.println("马上缓存url:" + url);
        String result = cacheRequest(conn, url, callback);
        System.out.println("获得initial的网页内容:" + result);
        System.out.println();

        assert result != null;

        System.out.println("测试是否已经缓存网页,给错误的callback");
        String result2 = cacheRequest(conn, url, null);
        System.out.println("result1和result2是否一致");
        System.out.println("result1:" + result + ";result2:" + result2);

        assert result.equals(result2);

        assert !canCache(conn, "http://test.com/");
        assert !canCache(conn, "http://test.com/?item=itemX&_=12345");
        System.out.println();
    }

    public String cacheRequest(Jedis conn, String request, Callback callback) {
        if (!canCache(conn, request)) {
            return callback != null ? callback.call(request) : null;
        }
        String cacheKey = "cache:" + hashRequest(request);
        String content = conn.get(cacheKey);
        if (content == null) {
            content = callback.call(request);
            conn.set(cacheKey, content);
        }

        return content;
    }

    public boolean canCache(Jedis conn, String request) {
        try {
            URL url = new URL(request);
            HashMap<String, String> params = new HashMap<>();
            if (url.getQuery() != null) {
                for (String param : url.getQuery().split("&")) {
                    String[] pair = param.split("=", 2);
                    params.put(pair[0], pair.length == 2 ? pair[1] : null);
                }
            }

            String itemId = extractItemId(params);
            if (itemId == null || isDynamic(params)) {
                return false;
            }
            Long rank = conn.zrank("viewed:", itemId);
            return rank != null && rank < 10000;
        } catch (MalformedURLException e) {
            return false;
        }
    }

    private boolean isDynamic(HashMap<String, String> params) {
        return params.containsKey("_");
    }

    private String extractItemId(HashMap<String, String> params) {
        return params.get("item");
    }

    private String hashRequest(String request) {
        return String.valueOf(request.hashCode());
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

    public void testShoppingCartCookies(Jedis conn) throws InterruptedException {
        System.out.println("\n--- 测试购物车cookie ---");
        String token = UUID.randomUUID().toString();

        System.out.println("马上更新session");
        updateToken(conn, token, "username", "itemX");
        System.out.println("添加一个商品到购物车");
        addToCart(conn, token, "itemY", 3);
        Map<String, String> r = conn.hgetAll("cart:" + token);
        System.out.println("当前购物车商品列表:");
        for (Map.Entry<String, String> entry : r.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        System.out.println();

        assert r.size() >= 1;

        System.out.println("清空session和购物车");
        CleanFullSessionsThread thread = new CleanFullSessionsThread(0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("清空线程仍然存活?");
        }

        r = conn.hgetAll("cart:" + token);
        System.out.println("现在购物车有");
        for (Map.Entry<String, String> entry : r.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() == 0;
    }

    public void addToCart(Jedis conn, String session, String item, int count) {
        if (count <= 0) {
            // 删除
            conn.hdel("cart:" + session, item);
        } else {
            // 设置
            conn.hset("cart:" + session, item, String.valueOf(count));
        }
    }

    public String checkToken(Jedis conn, String token) {
        return conn.hget("login:", token);
    }

    public void updateToken(Jedis conn, String token, String user, String item) {
        conn.hset("login:", token, user);

        long timestamp = System.currentTimeMillis() / 1000;
        conn.zadd("recent:", timestamp, token);

        if (item != null) {
            conn.zadd("viewed:" + token, timestamp, item);
            conn.zremrangeByRank("viewed:" + token, 0, -26);
            // 商品总浏览数
            conn.zincrby("viewed:", -1, item);
        }

    }

}
