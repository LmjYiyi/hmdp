package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Autowired
    SeckillVoucherMapper seckillVoucherMapper;
    @Autowired
    RedissonClient redissonClient;
    @Autowired
    StringRedisTemplate stringRedisTemplate;
    @Autowired
    VoucherOrderMapper voucherOrderMapper;
    @Autowired
    RedisIdWorker redisIdWorker;
    @Autowired
    ISeckillVoucherService seckillVoucherService;
    private IVoucherOrderService proxy;
    private static final DefaultRedisScript<Long> SECKILL;
    static {
        SECKILL = new DefaultRedisScript<>();
        SECKILL.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL.setResultType(Long.class);
    }
    private  static final ExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        EXECUTOR_SERVICE.submit(new VoucherOrderHandler());
    }
    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while (true){
                try {
                    //read message from stream
                    // xreadgroup group g1 c1 count 1 block 1000 streams stream.orders >
                    List<MapRecord<String,Object,Object>> messageList=  stringRedisTemplate.opsForStream().read(
                      Consumer.from("g1","c1"),
                      StreamReadOptions.empty().count(1).block(Duration.ofSeconds(1)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                   if(messageList==null||messageList.isEmpty()){
                       continue;
                   }
                    MapRecord<String,Object,Object> record = messageList.get(0);
                    Map<Object,Object> messageMap = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(
                            messageMap,
                            new VoucherOrder(),
                            true
                    );
                    handleVoucherOrder(voucherOrder);
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders","g1",record.getId());
                }catch (Exception e){
                    log.error("something go wrong in order service",e);
                    handlePendingList();
                }
            }
        }
    }
    private void handlePendingList(){
        while (true){
            try {
                //read messsage from pendingList
                //xreadgroup g1 c1 count 1 block 1000 stream stream.orders 0;
                 List<MapRecord<String,Object,Object>> messageList= stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1","c1"),
                        StreamReadOptions.empty().count(1).block(Duration.ofSeconds(1)),
                        StreamOffset.create("stream.orders",ReadOffset.from("0"))
                );
                if(messageList==null||messageList.isEmpty()){
                    break;
                }
                MapRecord<String,Object,Object> record= messageList.get(0);
                Map<Object,Object> messageMap = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(messageMap,new VoucherOrder(),true);
                handleVoucherOrder(voucherOrder);
                stringRedisTemplate.opsForStream().acknowledge("stream.orders","g1",record.getId());
            }catch (Exception e){
                log.error("something go wrong in orderService",e);
                try {
                    Thread.sleep(20);
                }catch (InterruptedException exception){
                    log.error("thread sleep error",exception);
                }
            }

        }
    }
    @Transactional
    @Override
    public Result seckillVoucher(Long voucherId) {
       Long userId = UserHolder.getUser().getId();
       long orderId = redisIdWorker.nextId(RedisConstants.SECKILL_VOUCHER_ORDER);
//       String stockKey = "seckill:stock:" +voucherId;
//       String orderKey = "seckill:order:" + voucherId;
//       String stock = stringRedisTemplate.opsForValue().get(stockKey);
//       if(Integer.parseInt(stock.trim())<=0){
//           return Result.fail("stock is not enough");
//       }
//       boolean isIn = stringRedisTemplate.opsForSet().isMember(orderKey,String.valueOf(userId));
//       if(isIn) {
//           return Result.fail("you have bought once");
//       }
//       stringRedisTemplate.opsForValue().decrement(stockKey);
//       stringRedisTemplate.opsForSet().add(orderKey,String.valueOf(userId));
//        Map<String, String> message = new HashMap<>();
//        message.put("userId", userId.toString());
//        message.put("voucherId", voucherId.toString());
//        message.put("id", String.valueOf(orderId));
//        stringRedisTemplate.opsForStream().add("stream.orders",message);
        //封装为一个lua脚本
       Long result = stringRedisTemplate.execute(
                   SECKILL,
                   Collections.emptyList(),
                   voucherId.toString(),userId.toString(),String.valueOf(orderId)

       );
       if(result!=null&&!result.equals(0L)){
           int r = result.intValue();
           return Result.fail(r==2?"you can not buy again":"stock is not enough");
       }
       IVoucherOrderService proxy  = (IVoucherOrderService)AopContext.currentProxy();
       this.proxy = proxy;
       return Result.ok();


    }
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        SeckillVoucher seckillVoucher = seckillVoucherMapper.selectById(voucherId);
//        if (seckillVoucher == null) {
//            return Result.fail("秒杀券不存在");
//        }
//
//        if(seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())){
//           return Result.fail("seckill is not start");
//       }
//       if(seckillVoucher.getEndTime().isBefore(LocalDateTime.now())){
//           return  Result.fail("seckill is expire");
//       }
//       if(seckillVoucher.getStock()<1){
//           return Result.fail("seckill is finished");
//       }
//       Long userId = UserHolder.getUser().getId();
//
////        RedisLock redisLock = new RedisLock(stringRedisTemplate,"order:"+userId);
////        boolean isLock = redisLock.tryLock(1200);
////        if(!isLock){
////            return Result.fail("you can only buy once");
////        }
////        try {
////            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
////            Result result =  proxy.createVoucherOrder(userId,voucherId);
////            return  result;
////        }finally {
////            redisLock.unlock();
////        }
//        RLock rLock = redissonClient.getLock("order:"+userId);
//         boolean isLock = rLock.tryLock();
//        if (!isLock){
//            return Result.fail("you can only buy once");
//        }
//        try {
//            IVoucherOrderService proxy =  (IVoucherOrderService)AopContext.currentProxy();
//            return  proxy.createVoucherOrder(userId,voucherId);
//        }finally {
//            rLock.unlock();
//        }
//
//
//    }

    @Transactional
    @Override
    public void createVoucherOrder(VoucherOrder voucherOrder){
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        int count = voucherOrderMapper.selectCount(
                new LambdaQueryWrapper<VoucherOrder>()
                        .eq(VoucherOrder::getUserId,userId)
        );
        if(count>=1){
            log.error("you can only buy once");
            return;
        }

//        // 2、用户是第一单，可以下单，秒杀券库存数量减一
//        boolean flag = seckillVoucherService.update(new LambdaUpdateWrapper<SeckillVoucher>()
//                .eq(SeckillVoucher::getVoucherId, voucherId)
//                .gt(SeckillVoucher::getStock, 0)
//                .setSql("stock = stock -1"));
//        if (!flag) {
//            throw new RuntimeException("秒杀券扣减失败");
//        }
        //自定义xml方法实现
        seckillVoucherMapper.selectVoucherForUpdate(voucherId);
        int row = seckillVoucherMapper.updateStock(voucherId);
        if(row<=0){
            throw new RuntimeException("seckill failed");
        }
        int r = voucherOrderMapper.insert(voucherOrder);
        if(r<=0){
            throw new RuntimeException("create order failed");
        }

    }
    private void handleVoucherOrder(VoucherOrder voucherOrder){
        Long userId = voucherOrder.getUserId();
        RLock rLock = redissonClient.getLock("lock:order:"+userId);
        boolean isLock = rLock.tryLock();
        if(!isLock){
            log.error("you can only buy once");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        }finally {
            rLock.unlock();
        }

    }
}
