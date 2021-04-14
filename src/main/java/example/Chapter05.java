package example;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.csv.CSVParser;
import org.javatuples.Pair;
import redis.clients.jedis.*;

import java.io.File;
import java.io.FileReader;
import java.text.Collator;
import java.text.SimpleDateFormat;
import java.util.*;

public class Chapter05 {
    /**
     * DEBUG, INFO, WARNING, ERROR, CRITICAL日志的安全级别
     */
    public static final String DEBUG = "debug";
    public static final String INFO = "info";
    public static final String WARNING = "warning";
    public static final String ERROR = "error";
    public static final String CRITICAL = "critical";

    public static final Collator COLLATOR = Collator.getInstance();

    public static final SimpleDateFormat TIMESTAMP =
            new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
    private static final SimpleDateFormat ISO_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    static {
        ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static final void main(String[] args)
            throws InterruptedException {
        new Chapter05().run();
    }

    public void run()
            throws InterruptedException {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

//        testLogRecent(conn);
//        testLogCommon(conn);
//        testCounters(conn);
//        testStats(conn);
//        testAccessTime(conn);
        testIpLookup(conn);
//        testIsUnderMaintenance(conn);
//        testConfig(conn);
    }

    public void testLogRecent(Jedis conn) {
        System.out.println("\n----- testLogRecent -----");
        System.out.println("Let's write a few logs to the recent log");
        for (int i = 0; i < 5; i++) {
            logRecent(conn, "test", "this is message " + i);
        }
        List<String> recent = conn.lrange("recent:test:info", 0, -1);
        System.out.println(
                "The current recent message log has this many messages: " +
                        recent.size());
        System.out.println("Those messages include:");
        for (String message : recent) {
            System.out.println(message);
        }
        assert recent.size() >= 5;
    }

    public void testAccessTime(Jedis conn)
            throws InterruptedException {
        System.out.println("\n----- testAccessTime -----");
        System.out.println("Let's calculate some access times...");
        AccessTimer timer = new AccessTimer(conn);
        for (int i = 0; i < 10; i++) {
            timer.start();
            Thread.sleep((int) ((.5 + Math.random()) * 1000));
            timer.stop("req-" + i);
        }
        System.out.println("The slowest access times are:");
        Set<Tuple> atimes = conn.zrevrangeWithScores("slowest:AccessTime", 0, -1);
        for (Tuple tuple : atimes) {
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert atimes.size() >= 10;
        System.out.println();
    }

    public void testIpLookup(Jedis conn) {
        System.out.println("\n----- testing ip查询 -----");
        String cwd = System.getProperty("user.dir");
        File blocks = new File(cwd + "/GeoLiteCity-Blocks.csv");
        File locations = new File(cwd + "/GeoLiteCity-Location.csv");
        if (!blocks.exists()) {
            System.out.println("********");
            System.out.println("GeoLiteCity-Blocks.csv 没找到: " + blocks);
            System.out.println("********");
            return;
        }
        if (!locations.exists()) {
            System.out.println("********");
            System.out.println("GeoLiteCity-Location.csv 没找到: " + locations);
            System.out.println("********");
            return;
        }

        System.out.println("插入ip到redis,Importing IP addresses to Redis... (this may take a while)");
//        importIpsToRedis(conn, blocks);
        long ranges = conn.zcard("ip2cityid:");
        System.out.println("ip共有:Loaded ranges into Redis: " + ranges);
        assert ranges > 1000;
        System.out.println();

        System.out.println("导入location到redis,Importing Location lookups to Redis... (this may take a while)");
//        importCitiesToRedis(conn, locations);
        long cities = conn.hlen("cityid2city:");
        System.out.println("city共有:Loaded city lookups into Redis:" + cities);
        assert cities > 1000;
        System.out.println();

        System.out.println("查location:Let's lookup some locations!");
        for (int i = 0; i < 5; i++) {
            String ip =
                    randomOctet(255) + '.' +
                            randomOctet(256) + '.' +
                            randomOctet(256) + '.' +
                            randomOctet(256);
            System.out.println("ip地址为:" + ip);
            System.out.println(Arrays.toString(findCityByIp(conn, ip)));
        }
    }

    public void testIsUnderMaintenance(Jedis conn)
            throws InterruptedException {
        System.out.println("\n----- testIsUnderMaintenance -----");
        System.out.println("Are we under maintenance (we shouldn't be)? " + isUnderMaintenance(conn));
        conn.set("is-under-maintenance", "yes");
        System.out.println("We cached this, so it should be the same: " + isUnderMaintenance(conn));
        Thread.sleep(1000);
        System.out.println("But after a sleep, it should change: " + isUnderMaintenance(conn));
        System.out.println("Cleaning up...");
        conn.del("is-under-maintenance");
        Thread.sleep(1000);
        System.out.println("Should be False again: " + isUnderMaintenance(conn));
    }

    public void testConfig(Jedis conn) {
        System.out.println("\n----- testConfig -----");
        System.out.println("Let's set a config and then get a connection from that config...");
        Map<String, Object> config = new HashMap<String, Object>();
        config.put("db", 15);
        setConfig(conn, "redis", "test", config);

        Jedis conn2 = redisConnection("test");
        System.out.println(
                "We can run commands from the configured connection: " + (conn2.info() != null));
    }

    public void logRecent(Jedis conn, String name, String message) {
        logRecent(conn, name, message, INFO);
    }

    /**
     * 记录最新日志
     * - lpush将日志推入到列表
     *
     * @param conn
     * @param name
     * @param message
     * @param severity: 安全级别
     */
    public void logRecent(Jedis conn, String name, String message, String severity) {
        String destination = "recent:" + name + ':' + severity;
        Pipeline pipe = conn.pipelined();
        pipe.lpush(destination, TIMESTAMP.format(new Date()) + ' ' + message);
        pipe.ltrim(destination, 0, 99);
        pipe.sync();
    }

    public void testLogCommon(Jedis conn) {
        System.out.println("\n----- testLogCommon -----");
        System.out.println("Let's write some items to the common log");
        // 记录常见日志
        for (int count = 1; count < 6; count++) {
            // 日志出现count次
            for (int i = 0; i < count; i++) {
                logCommon(conn, "test", "message-" + count);
            }
        }
        Set<Tuple> common = conn.zrevrangeWithScores("common:test:info", 0, -1);
        System.out.println("The current number of common messages is: " + common.size());
        System.out.println("Those common messages are:");
        // 打印所有常见日志
        for (Tuple tuple : common) {
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert common.size() >= 5;
    }

    public void logCommon(Jedis conn, String name, String message) {
        logCommon(conn, name, message, INFO, 5000);
    }

    /**
     * zset中:消息作为成员,出现频率作为分值
     */
    public void logCommon(
            Jedis conn, String name, String message, String severity, int timeout) {
        // redis-zset:最常见的日志, 日志+频率(score)
        String commonDest = "common:" + name + ':' + severity;
        // redis-string:开始的小时数
        String startKey = commonDest + ":start";
        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end) {
            conn.watch(startKey);
            // 实际的小时数,比如:12
            String hourStart = ISO_FORMAT.format(new Date());
            // 开始的小时数,比如11
            String existing = conn.get(startKey);

            Transaction trans = conn.multi();
            if (existing != null && COLLATOR.compare(existing, hourStart) < 0) {
                // 保留最常见的日志上一个版本
                trans.rename(commonDest, commonDest + ":last");
                // 保留上一个日志开始的小时数
                trans.rename(startKey, commonDest + ":pstart");
                // 再把开始的小时数,设置为12. 就实现了每小时,更新最常见的日志
                trans.set(startKey, hourStart);
            }

            trans.zincrby(commonDest, 1, message);

            // recentDest = "recent:test:info"
            String recentDest = "recent:" + name + ':' + severity;
            trans.lpush(recentDest, TIMESTAMP.format(new Date()) + ' ' + message);
            trans.ltrim(recentDest, 0, 99);
            List<Object> results = trans.exec();
//             null response indicates that the transaction was aborted due to
//             the watched key changing.
            if (results == null) {
                continue;
            }
            return;
        }
    }

    public void testCounters(Jedis conn)
            throws InterruptedException {
        System.out.println("\n----- testCounters -----");
        System.out.println("Let's update some counters for now and a little in the future");
        long now = System.currentTimeMillis() / 1000;
        for (int i = 0; i < 10; i++) {
            int count = (int) (Math.random() * 5) + 1;
            updateCounter(conn, "test", count, now + i);
        }

        List<Pair<Integer, Integer>> counter = getCounter(conn, "test", 1);
        System.out.println("We have some per-second counters: " + counter.size());
        System.out.println("These counters include:");
        for (Pair<Integer, Integer> count : counter) {
            System.out.println("  " + count);
        }
        assert counter.size() >= 10;

        counter = getCounter(conn, "test", 5);
        System.out.println("We have some per-5-second counters: " + counter.size());
        System.out.println("These counters include:");
        for (Pair<Integer, Integer> count : counter) {
            System.out.println("  " + count);
        }
        assert counter.size() >= 2;
        System.out.println();

        System.out.println("Let's clean out some counters by setting our sample count to 0");
        CleanCountersThread thread = new CleanCountersThread(0, 2 * 86400000);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        thread.interrupt();
        counter = getCounter(conn, "test", 86400);
        System.out.println("Did we clean out all of the counters? " + (counter.size() == 0));
        assert counter.size() == 0;
    }

    public void updateCounter(Jedis conn, String name, int count) {
        updateCounter(conn, name, count, System.currentTimeMillis() / 1000);
    }

    // 计数器精度数组,比如1s的点击量,5s的点击量...
    public static final int[] PRECISION = new int[]{1, 5, 60, 300, 3600, 18000, 86400};

    // 更新计数器数据
    public void updateCounter(Jedis conn, String name, int count, long now) {
        Transaction trans = conn.multi();
        for (int prec : PRECISION) {
            long pnow = (now / prec) * prec;
            String hash = String.valueOf(prec) + ':' + name;
            // 放入known:--zset中
            trans.zadd("known:", 0, hash);
            // 增加计数器的值(点击量)
            trans.hincrBy("count:" + hash, String.valueOf(pnow), count);
        }
        trans.exec();
    }

    // 获取计数器数据
    public List<Pair<Integer, Integer>> getCounter(
            Jedis conn, String name, int precision) {
        String hash = String.valueOf(precision) + ':' + name;
        Map<String, String> data = conn.hgetAll("count:" + hash);
        ArrayList<Pair<Integer, Integer>> results =
                new ArrayList<Pair<Integer, Integer>>();
        for (Map.Entry<String, String> entry : data.entrySet()) {
            results.add(new Pair<Integer, Integer>(
                    Integer.parseInt(entry.getKey()),
                    Integer.parseInt(entry.getValue())));
        }
        Collections.sort(results);
        return results;
    }

    public void testStats(Jedis conn) {
        System.out.println("\n----- testStats -----");
        System.out.println("Let's add some data for our statistics!");
        List<Object> r = null;
        for (int i = 0; i < 5; i++) {
            double value = (Math.random() * 11) + 5;
            r = updateStats(conn, "temp", "example", value);
        }
        System.out.println("We have some aggregate statistics: " + r);
        Map<String, Double> stats = getStats(conn, "temp", "example");
        System.out.println("Which we can also fetch manually:");
        System.out.println(stats);
        assert stats.get("count") >= 5;
    }

    /**
     *
     * @param conn
     * @param context
     * @param type
     * @param value
     * @return List<Object>: 0:count-计数;1:sum-总和;2:sumsq-平方和的总数
     */
    public List<Object> updateStats(Jedis conn, String context, String type, double value) {
        int timeout = 5000;
        // 统计数据的键
        String destination = "stats:" + context + ':' + type;
        String startKey = destination + ":start";
        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end) {
            /**
             * 处理当前这一小时和上一个小时的数据
             */
            conn.watch(startKey);
            String hourStart = ISO_FORMAT.format(new Date());

            String existing = conn.get(startKey);
            Transaction trans = conn.multi();
            if (existing != null && COLLATOR.compare(existing, hourStart) < 0) {
                trans.rename(destination, destination + ":last");
                trans.rename(startKey, destination + ":pstart");
                trans.set(startKey, hourStart);
            }


            String tkey1 = UUID.randomUUID().toString();
            String tkey2 = UUID.randomUUID().toString();
            // 将值添加到临时键里面
            trans.zadd(tkey1, value, "min");
            trans.zadd(tkey2, value, "max");

            // 使用聚合min,临时键1和统计数据键 进行交集计算
            trans.zunionstore(
                    destination,
                    new ZParams().aggregate(ZParams.Aggregate.MIN),
                    destination, tkey1);
            // 使用聚合max,临时键2和统计数据键 进行交集计算
            trans.zunionstore(
                    destination,
                    new ZParams().aggregate(ZParams.Aggregate.MAX),
                    destination, tkey2);

            // 删除临时键1`2
            trans.del(tkey1, tkey2);
            // count++
            trans.zincrby(destination, 1, "count");
            // sum += value
            trans.zincrby(destination, value, "sum");
            // sumsq += value*2
            trans.zincrby(destination, value * value, "sumsq");

            List<Object> results = trans.exec();
            if (results == null) {
                continue;
            }
            // 返回基本的计数信息
            return results.subList(results.size() - 3, results.size());
        }
        return null;
    }

    public Map<String, Double> getStats(Jedis conn, String context, String type) {
        // 统计数据键
        String key = "stats:" + context + ':' + type;
        Map<String, Double> stats = new HashMap<String, Double>();
        Set<Tuple> data = conn.zrangeWithScores(key, 0, -1);
        for (Tuple tuple : data) {
            stats.put(tuple.getElement(), tuple.getScore());
        }

        // 计算平均数
        stats.put("average", stats.get("sum") / stats.get("count"));
        // 计算标准差
        double numerator = stats.get("sumsq") - Math.pow(stats.get("sum"), 2) / stats.get("count");
        double count = stats.get("count");
        stats.put("stddev", Math.pow(numerator / (count > 1 ? count - 1 : 1), .5));
        return stats;
    }

    private long lastChecked;
    private boolean underMaintenance;

    public boolean isUnderMaintenance(Jedis conn) {
        if (lastChecked < System.currentTimeMillis() - 1000) {
            lastChecked = System.currentTimeMillis();
            String flag = conn.get("is-under-maintenance");
            underMaintenance = "yes".equals(flag);
        }

        return underMaintenance;
    }

    public void setConfig(
            Jedis conn, String type, String component, Map<String, Object> config) {
        Gson gson = new Gson();
        conn.set("config:" + type + ':' + component, gson.toJson(config));
    }

    private static final Map<String, Map<String, Object>> CONFIGS =
            new HashMap<String, Map<String, Object>>();
    private static final Map<String, Long> CHECKED = new HashMap<String, Long>();

    @SuppressWarnings("unchecked")
    public Map<String, Object> getConfig(Jedis conn, String type, String component) {
        int wait = 1000;
        String key = "config:" + type + ':' + component;

        Long lastChecked = CHECKED.get(key);
        if (lastChecked == null || lastChecked < System.currentTimeMillis() - wait) {
            CHECKED.put(key, System.currentTimeMillis());

            String value = conn.get(key);
            Map<String, Object> config = null;
            if (value != null) {
                Gson gson = new Gson();
                config = (Map<String, Object>) gson.fromJson(
                        value, new TypeToken<Map<String, Object>>() {
                        }.getType());
            } else {
                config = new HashMap<String, Object>();
            }

            CONFIGS.put(key, config);
        }

        return CONFIGS.get(key);
    }

    public static final Map<String, Jedis> REDIS_CONNECTIONS =
            new HashMap<String, Jedis>();

    public Jedis redisConnection(String component) {
        Jedis configConn = REDIS_CONNECTIONS.get("config");
        if (configConn == null) {
            configConn = new Jedis("localhost");
            configConn.select(15);
            REDIS_CONNECTIONS.put("config", configConn);
        }

        String key = "config:redis:" + component;
        Map<String, Object> oldConfig = CONFIGS.get(key);
        Map<String, Object> config = getConfig(configConn, "redis", component);

        if (!config.equals(oldConfig)) {
            Jedis conn = new Jedis("localhost");
            if (config.containsKey("db")) {
                conn.select(((Double) config.get("db")).intValue());
            }
            REDIS_CONNECTIONS.put(key, conn);
        }

        return REDIS_CONNECTIONS.get(key);
    }

    /**
     * 这个函数执行时需要输入GeoLiteCity-Blocks.csv文件
     * GeoLiteCity-Blocks.csv中第一列startIp, 第二列endIp, 第三列cityId
     * redis中ip2cityid: cityid为member,ip为score
     */
    public void importIpsToRedis(Jedis conn, File file) {
        FileReader reader = null;
        try {
            // 读取流
            reader = new FileReader(file);
            CSVParser parser = new CSVParser(reader);
            int count = 0;
            String[] line = null;
            // 迭代获取每一行
            while ((line = parser.getLine()) != null) {
                /**
                 * 按需将IP地址转换为分值
                 */
                String startIp = line.length > 1 ? line[0] : "";
                if (startIp.toLowerCase().indexOf('i') != -1) {
                    continue;
                }
                int score = 0;
                if (startIp.indexOf('.') != -1) {
                    score = ipToScore(startIp);
                } else {
                    try {
                        score = Integer.parseInt(startIp, 10);
                    } catch (NumberFormatException nfe) {
                        // 略过不正确的条目
                        continue;
                    }
                }


                // 构建唯一城市ID
                String cityId = line[2] + '_' + count;
                // 将城市ID及其对应的IP地址分值添加到有序集合里面
                conn.zadd("ip2cityid:", score, cityId);
                count++;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                reader.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * 这个函数在执行时需要输入GeoLiteCity-Location.csv文件
     * GeoLiteCity-Location.csv中: 第一列:cityId, 第二列:country,第三列区域,第四列城市
     * redis的cityid2city:中: cityid为key, 国`地区`城市为键
     */
    public void importCitiesToRedis(Jedis conn, File file) {
        Gson gson = new Gson();
        FileReader reader = null;
        try {
            // 读取流
            reader = new FileReader(file);
            CSVParser parser = new CSVParser(reader);
            String[] line = null;
            while ((line = parser.getLine()) != null) {
                if (line.length < 4 || !Character.isDigit(line[0].charAt(0))) {
                    continue;
                }
                // 将城市id,国家`区域`城市
                String cityId = line[0];
                String country = line[1];
                String region = line[2];
                String city = line[3];
                String json = gson.toJson(new String[]{city, region, country});
                // 将城市信息添加到redis里面
                conn.hset("cityid2city:", cityId, json);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                reader.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public int ipToScore(String ipAddress) {
        int score = 0;
        for (String v : ipAddress.split("\\.")) {
            // 256必须大于ip分段范围
            // 只有乘以>=256,才能使所有分段范围都能还原
            score = score * 256 + Integer.parseInt(v, 10);
        }
        return score;
    }

    public String randomOctet(int max) {
        return String.valueOf((int) (Math.random() * max));
    }

    public String[] findCityByIp(Jedis conn, String ipAddress) {
        // 将ip地址转换成分值以便执行zrevrangebyscore命令
        int score = ipToScore(ipAddress);
        Set<String> results = conn.zrevrangeByScore("ip2cityid:", score, 0, 0, 1);
        if (results.size() == 0) {
            return null;
        }

        // 查找唯一的城市id
        String cityId = results.iterator().next();
        // 将唯一城市ID转换为普通城市ID
        cityId = cityId.substring(0, cityId.indexOf('_'));
        // 从散列里面取出城市信息
        return new Gson().fromJson(conn.hget("cityid2city:", cityId), String[].class);
    }

    /**
     * 清理计数器线程
     */
    public class CleanCountersThread
            extends Thread {
        private Jedis conn;
        private int sampleCount = 100;
        private boolean quit;
        private long timeOffset; // used to mimic a time in the future.

        public CleanCountersThread(int sampleCount, long timeOffset) {
            this.conn = new Jedis("localhost");
            this.conn.select(15);
            this.sampleCount = sampleCount;
            this.timeOffset = timeOffset;
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            // 为了平等处理更新频率各不相同的计数器,需要记录清理操作的次数
            int passes = 0;
            // 持续清理,直到手动退出
            while (!quit) {
                // 用于记录清理时长
                long start = System.currentTimeMillis() + timeOffset;
                int index = 0;
                // 遍历所有已知的计数器
                while (index < conn.zcard("known:")) {
                    Set<String> hashSet = conn.zrange("known:", index, index);
                    index++;
                    if (hashSet.size() == 0) {
                        break;
                    }
                    // 获取当前计数器的键
                    String hash = hashSet.iterator().next();
                    // 取得当前计数器的精度
                    int prec = Integer.parseInt(hash.substring(0, hash.indexOf(':')));
                    // 如果精度小于60s,bprec为1
                    int bprec = (int) Math.floor(prec / 60);
                    if (bprec == 0) {
                        bprec = 1;
                    }
                    // 如果计数器不需要清理,就检查下一个计数器.例如清理程序只循环了3次,那么精度为5分钟
                    // 不需要进行清理.
                    if ((passes % bprec) != 0) {
                        continue;
                    }

                    // 计数器的hash键
                    String hkey = "count:" + hash;
                    // 根据精度`样本数量,计算出保留的时间
                    String cutoff = String.valueOf(
                            ((System.currentTimeMillis() + timeOffset) / 1000) - sampleCount * prec);
                    ArrayList<String> samples = new ArrayList<String>(conn.hkeys(hkey));
                    Collections.sort(samples);
                    // 计算需要移除的样本数量
                    int remove = bisectRight(samples, cutoff);

                    if (remove != 0) {
                        conn.hdel(hkey, samples.subList(0, remove).toArray(new String[0]));
                        // 如果计数器清理空了,那么就从known:中清理掉
                        // 注意index--
                        if (remove == samples.size()) {
                            conn.watch(hkey);
                            if (conn.hlen(hkey) == 0) {
                                Transaction trans = conn.multi();
                                trans.zrem("known:", hash);
                                trans.exec();
                                index--;
                            } else {
                                conn.unwatch();
                            }
                        }
                    }
                }

                passes++;
                long duration = Math.min(
                        (System.currentTimeMillis() + timeOffset) - start + 1000, 60000);
                try {
                    // 未耗尽60s,补足60s,大于60s,再休眠1s
                    sleep(Math.max(60000 - duration, 1000));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // mimic python's bisect.bisect_right
        public int bisectRight(List<String> values, String key) {
            int index = Collections.binarySearch(values, key);
            return index < 0 ? Math.abs(index) - 1 : index + 1;
        }
    }

    /**
     * 访问时长计时器
     */
    public class AccessTimer {
        private Jedis conn;
        private long start;

        public AccessTimer(Jedis conn) {
            this.conn = conn;
        }

        public void start() {
            // 设置开始访问时间
            start = System.currentTimeMillis();
        }

        public void stop(String context) {
            // 计算访问使用了多少时间
            long delta = System.currentTimeMillis() - start;
            // 更新统计数据,并返回count,sum,sumsq
            List<Object> stats = updateStats(conn, context, "AccessTime", delta / 1000.0);
            // 计算平均时长
            double average = (Double) stats.get(1) / (Double) stats.get(0);

            Transaction trans = conn.multi();
            // 将平均时长添加到<slowest:AccessTime--访问最慢榜>中
            trans.zadd("slowest:AccessTime", average, context);
            // 访问最慢榜只保留前100位
            trans.zremrangeByRank("slowest:AccessTime", 0, -101);
            trans.exec();
        }
    }
}
