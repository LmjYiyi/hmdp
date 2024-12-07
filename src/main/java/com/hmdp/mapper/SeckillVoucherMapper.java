package com.hmdp.mapper;

import com.hmdp.entity.SeckillVoucher;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;

public interface SeckillVoucherMapper extends BaseMapper<SeckillVoucher> {
        int updateStock(@Param("voucher_id") Long voucherId);
        SeckillVoucher selectVoucherForUpdate(@Param("voucher_id") Long voucherId);
}
