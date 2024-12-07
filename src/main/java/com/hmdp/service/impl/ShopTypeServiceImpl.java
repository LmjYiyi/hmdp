package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;


@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    StringRedisTemplate stringRedisTemplate;
    @Autowired
    ShopTypeMapper shopTypeMapper;
    @Override
    public Result queryTypeList(){
        String key = CACHE_SHOP_KEY+ UUID.randomUUID().toString();
       String shopTypeJson =  stringRedisTemplate.opsForValue().get(key);
        List<ShopType> typeList = null;
        if(StrUtil.isNotBlank(shopTypeJson)){
            typeList =  JSONUtil.toList(shopTypeJson,ShopType.class);
            return Result.ok(typeList);
        }else{
            LambdaQueryWrapper<ShopType> wrapper = new LambdaQueryWrapper();
            wrapper.orderByAsc(ShopType::getSort);
            typeList  = shopTypeMapper.selectList(wrapper);
            if(typeList==null){
                return Result.fail("shopeType is not exist");
            }
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(typeList));
            return Result.ok(typeList);
        }

    }
}
