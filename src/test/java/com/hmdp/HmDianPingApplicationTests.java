package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    IShopService shopService;
    @Resource
    StringRedisTemplate stringRedisTemplate;

    private ExecutorService es = Executors.newFixedThreadPool(500);

    /**
     * 测试分布式ID生成器的性能，以及可用性
     */
    @Test
    public void testNextId() throws InterruptedException {
        // 使用CountDownLatch让线程同步等待
        CountDownLatch latch = new CountDownLatch(300);
        // 创建线程任务
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id = " + id);
            }
            // 等待次数-1
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        // 创建300个线程，每个线程创建100个id，总计生成3w个id
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        // 线程阻塞，直到计数器归0时才全部唤醒所有线程
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("生成3w个id共耗时" + (end - begin) + "ms");
    }
    @Test
    public void loadShopData(){
        List<Shop> shopList = shopService.list();
        Map<Long,List<Shop>> shopMap = new HashMap<>();
        for(Shop shop:shopList){
            Long typeId = shop.getTypeId();
            if(shopMap.containsKey(typeId)){
                shopMap.get(typeId).add(shop);
            }else{
                shopMap.put(typeId,new ArrayList<>(Arrays.asList(shop)));
            }
        }
        for (Map.Entry<Long,List<Shop>> shopEntry:shopMap.entrySet()){
            Long typeId  = shopEntry.getKey();
            List<Shop> shops = shopEntry.getValue();
            String key = SHOP_GEO_KEY + typeId;
            for(Shop shop:shops){
                stringRedisTemplate.opsForGeo().add(
                        key,
                        new Point(shop.getX(),shop.getY()),
                        shop.getId().toString()
                );
            }
        }
    }
    @Test
    public void testHyperLogLog() {
        String[] values = new String[1000];
        // 批量保存100w条用户记录，每一批1个记录
        int j = 0;
        for (int i = 0; i < 1000000; i++) {
            j = i % 1000;
            values[j] = "user_" + i;
            if (j == 999) {
                //pfadd key element []
                stringRedisTemplate.opsForHyperLogLog().add("hpll",values);
            }
        }
        // 统计数量
        //pfcount key []
        System.out.println(stringRedisTemplate.opsForHyperLogLog().size("hpll"));

    }

}
