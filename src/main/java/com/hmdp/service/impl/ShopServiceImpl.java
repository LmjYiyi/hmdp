package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;


@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private ShopMapper shopMapper;
    @Override
    public Result queryById(Long id){

         String key = CACHE_SHOP_KEY+id;
         Result result = getShopFromCache(key);
         if(result!=null)
            return result;
         String lockKey = LOCK_SHOP_KEY+id;
         try{
                 boolean isLock = tryLock(lockKey);
                 if(!isLock){
                    Thread.sleep(50);
                    return queryById(id);
                }
                 result = getShopFromCache(key);
                 if(result==null){
                     Shop shop = shopMapper.selectById(id);
                     if(shop==null){
                        stringRedisTemplate.opsForValue().set(key,"null",CACHE_NULL_TTL,TimeUnit.MINUTES);
                        return Result.fail("shop is not exist");
                     }
                    stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL,TimeUnit.MINUTES);
                    return Result.ok(shop);
                 }else{
                    return result;
                }
         }catch (Exception e){
            e.printStackTrace();
         }finally {
            unlock(lockKey);
         }
         return null;

    }
    @Override
    @Transactional
    public Result update(Shop shop){
        String key = CACHE_SHOP_KEY+shop.getId();
       int row =  shopMapper.updateById(shop);
       if(row<=0)
            throw new RuntimeException("database update failed");
        boolean redisDeleteFlag= stringRedisTemplate.delete(key);
        if(!redisDeleteFlag)
            throw new RuntimeException("redis delete failed");
        return Result.ok();
    }

    @Override
    public Result queryShopByTypeId(Integer typeId, Integer current, Double x, Double y) {
        if(x==null||y==null){
            Page<Shop> page = query().eq("type_id",typeId).
                    page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        int start = (current-1)*SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current*SystemConstants.DEFAULT_PAGE_SIZE;
        String key = SHOP_GEO_KEY+typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().radius(
                key,
                new Circle(new Point(x, y), new Distance(5000, RedisGeoCommands.DistanceUnit.METERS)),
                RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs().limit(end)
        );
        if(results==null)
            return Result.ok(Collections.emptyList());
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size()<=start)
            return Result.ok(Collections.emptyList());
        Map<String,Distance> distanceMap = new HashMap<>();
        List<Long> ids = new ArrayList<>();
        list.stream().skip(start).forEach(
                result->{
                    String shopIdStr = result.getContent().getName();
                    Distance distance = result.getDistance();
                    ids.add(Long.valueOf(shopIdStr));
                    distanceMap.put(shopIdStr,distance);
                }
        );
        // 根据店铺ids查询出店铺数据
        String idStr = StrUtil.join(",", ids);
        // 查寻出所有符合条件的店铺数据（这里需要利用ORDER BY FIELD确保id的有序性）
        List<Shop> shopList = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for(Shop shop:shopList){
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shopList);
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
    private Result getShopFromCache(String key){
        String shopJsonStr = stringRedisTemplate.opsForValue().get(key);
        if(shopJsonStr!=null){
            if(shopJsonStr.equals("null")){
                return Result.fail("shop is not exist");
            }
            Shop shop = JSONUtil.toBean(shopJsonStr,Shop.class);
            return Result.ok(shop);
        }
        return null;
    }
    public void reBuildCache(Long id,Long expireSeconds){
        String key = CACHE_SHOP_KEY + id;
        //check from database
        Shop shop = shopMapper.selectById(id);
        if(shop==null){
            stringRedisTemplate.opsForValue().set(key,"null",CACHE_NULL_TTL,TimeUnit.MINUTES);
        }
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }
    private RedisData getRedisData(String key){
        String jsonStr = stringRedisTemplate.opsForValue().get(key);
        return JSONUtil.toBean(jsonStr,RedisData.class);
    }


}




/**logical delete
 String key = CACHE_SHOP_KEY+id;
 String jsonStr = stringRedisTemplate.opsForValue().get(key);
 if(jsonStr.equals("null"))
 return Result.fail("shop is not exist");
 RedisData redisData = JSONUtil.toBean(jsonStr,RedisData.class);
 JSONObject data = (JSONObject) redisData.getData();
 Shop shop = JSONUtil.toBean(data, Shop.class);
 LocalDateTime expireTime = redisData.getExpireTime();
 if(expireTime.isAfter(LocalDateTime.now()))
 //has not expire
 return Result.ok(shop);
 String lockKey = LOCK_SHOP_KEY+id;
 boolean isLock = tryLock(lockKey);
 if(isLock){
 CACHE_REBUILD_EXECUTOR.submit(()->{
 try {
 reBuildCache(id,CACHE_SHOP_LOGICAL_TTL);
 }catch (Exception e){
 e.printStackTrace();
 }finally {
 unlock(lockKey);
 }
 });
 }
 jsonStr = stringRedisTemplate.opsForValue().get(key);
 if(jsonStr.equals("null"))
 return Result.fail("shop is not exist");
 redisData = JSONUtil.toBean(jsonStr,RedisData.class);
 data = (JSONObject) redisData.getData();
 shop = JSONUtil.toBean(data, Shop.class);
 expireTime = redisData.getExpireTime();
 if(expireTime.isAfter(LocalDateTime.now())){
 //has not expire
 return Result.ok(shop);
 }
 return  Result.ok(shop);
 */