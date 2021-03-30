package c04;

import com.alibaba.fastjson.JSON;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Set;

public class Store {
    public static void main(String[] args) {
        Jedis conn = new Jedis("localhost", 6379);

        testListItem(conn);
//        testPurchaseItem(conn);
    }

    public static void testPurchaseItem(Jedis conn) {
        String sellerId = "001";
        String itemId = "A";
        String sellerUser = "users:" + sellerId;

        String buyerId = "002";
        String buyerInventory = "inventory:" + buyerId;
        String buyerUser = "users:" + buyerId;

        String market = "market:";
        String itemInMarket = String.format("%s.%s", itemId, sellerId);

        // 50块钱在商店
        conn.zadd(market, 50., itemInMarket);

        conn.hset(sellerUser, "funds", "50");
        conn.hset(buyerUser, "funds", "50");

        boolean purchasedItem = purchaseItem(conn, buyerId, itemId, sellerId, 50.);
        System.out.println("购买成功了?" + purchasedItem);
        System.out.println("买家还有多少钱?");
        System.out.println(conn.hget(buyerUser, "funds"));
        System.out.println("买家的背包呢?");
        System.out.println(conn.smembers(buyerInventory));
        System.out.println("卖家还有多少钱?");
        System.out.println(conn.hget(sellerUser, "funds"));
    }

    public static boolean purchaseItem(Jedis conn, String buyerId, String itemId, String sellerId, double lprice) {
        String buyerUser = String.format("users:%s", buyerId);
        String sellerUser = String.format("users:%s", sellerId);
        String inventory = String.format("inventory:%s", buyerId);
        String itemId$SellerId = String.format("%s.%s", itemId, sellerId);

        long end = System.currentTimeMillis() + 10000;

        while (System.currentTimeMillis() < end) {
            //盯着商店别东西卖了,用户被没钱了
            conn.watch("market:", buyerUser);

            double priceInMarket = conn.zscore("market:", itemId$SellerId);
            double buyerFunds = Double.parseDouble(conn.hget(buyerUser, "funds"));
            if (priceInMarket != buyerFunds || priceInMarket > buyerFunds) {
                conn.unwatch();
                return false;
            }

            double sellerFunds = Double.parseDouble(conn.hget(sellerUser, "funds"));

            //开始事务
            Transaction trans = conn.multi();
            //把钱从buyer到seller
            trans.hset(buyerUser, "funds", String.valueOf(buyerFunds - lprice));
            trans.hset(sellerUser, "funds", String.valueOf(sellerFunds + lprice));
            //给东西
            trans.sadd(inventory, itemId);
            trans.zrem("market:", itemId);
            List<Object> results = trans.exec();
            if (results == null) continue;
            return true;
        }
        return true;
    }

    public static void testListItem(Jedis conn) {
        String sellerId = "003";
        String itemId = "C";
        String sellerInventory = "inventory:" + sellerId;
        String sellerUser = String.format("users:%s", sellerId);

        conn.sadd(sellerInventory, itemId);
        Set<String> sellerInventoryDetails = conn.smembers(sellerInventory);
        System.out.println("背包里有:");
        System.out.println(JSON.toJSONString(sellerInventoryDetails));
        assert sellerInventoryDetails.size() > 0;

        System.out.println("上架中...");
        boolean listItemSuccess = listItem(conn, itemId, sellerId, 50.);
        System.out.println("上架是否成功?" + listItemSuccess);
        Set<Tuple> marketDetails = conn.zrangeWithScores("market", 0, -1);
        System.out.println("商店有:");
        System.out.println(JSON.toJSONString(marketDetails));
        assert marketDetails.size() > 0;
    }

    public static boolean listItem(Jedis conn, String itemId, String sellerId, Double price) {
        //包裹
        String inventory = String.format("inventory:%s", sellerId);
        //商店的商品+价格
        String itemId$SellerId = String.format("%s.%s", itemId, sellerId);
        //监视时间
        long end = System.currentTimeMillis() + 5000;

        while (System.currentTimeMillis() < end) {
            //监视卖家包裹
            conn.watch(inventory);
            //如果包裹没有物品,直接unwatch
            if (!conn.sismember(inventory, itemId)) {
                conn.unwatch();
                return false;
            }

            //开始事务
            Transaction trans = conn.multi();
            trans.zadd("market", price, itemId$SellerId);
            trans.srem(inventory, itemId);
            List<Object> result = trans.exec();
            if (result == null) {
                continue;
            }
            return true;
        }
        return false;
    }
}
