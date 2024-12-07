package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;


@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Autowired
    IUserService userService;
    @Autowired
    BlogMapper blogMapper;
    @Autowired
    FollowMapper followMapper;
    @Autowired
    StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryHotBlog(Integer current) {
        Page<Blog> page = this.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        List<Blog> records = page.getRecords();
        records.forEach(
                blog -> {
                    this.queryUserByBlog(blog);
                    this.isBlokLiked(blog);
                }
        );
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = blogMapper.selectById(id);
        if(blog==null)
            return Result.fail("blog is not exist");
        queryUserByBlog(blog);
        isBlokLiked(blog);
        return Result.ok(blog);
    }
    private void queryUserByBlog(Blog blog){
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result likeBlog(Long id) {
        Long userId = UserHolder.getUser().getId();
        String key = BLOG_LIKED_KEY+id;
        Double score = stringRedisTemplate.opsForZSet().score(key,userId.toString());
        boolean result;
        if(score==null){
            result = this.update(
                    new LambdaUpdateWrapper<Blog>().eq(Blog::getId,id)
                            .setSql("liked=liked+1"));
            if(result){
                //zadd key score member
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }
        }else{
            result = this.update(
                    new LambdaUpdateWrapper<Blog>().eq(Blog::getId,id)
                            .setSql("liked=liked-1"));
            if(result){
                stringRedisTemplate.opsForZSet().remove(key,userId.toString());
            }
        }

        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY+id;
        //top5: zrange key 0 4
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key,0,4);
        if(top5==null||top5.isEmpty())
            return Result.ok(Collections.emptyList());
        List<Long> idList = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        List<UserDTO> userDTOList = userService.listByIds(idList)
                .stream().map(user -> BeanUtil.copyProperties(user,UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOList);
    }

    @Override
    public Result saveBlog(Blog blog) {
        Long userId = UserHolder.getUser().getId();
        blog.setUserId(userId);
        boolean isSuccess = this.save(blog);
        if(!isSuccess)
            return Result.fail("blog save failed");
        List<Follow> follows = followMapper.selectList(
                new LambdaQueryWrapper<Follow>()
                        .eq(Follow::getFollowUserId,userId)
        );
        for (Follow follow:follows){
            Long id = follow.getUserId();
            String key = FEED_KEY+id;
            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
        }
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        Long userId = UserHolder.getUser().getId();
        String key = FEED_KEY+userId;
        //zrevrangebyscore key max min limot offset count
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet().
                reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        if(typedTuples==null||typedTuples.isEmpty())
            return Result.ok();

        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int cnt = 1;
        for(ZSetOperations.TypedTuple<String> typedTuple:typedTuples){
            ids.add(Long.valueOf(typedTuple.getValue()));
            long time = typedTuple.getScore().longValue();
            if(time==minTime){
                cnt++;
            }else{
                minTime = time;
                cnt=1;
            }
        }
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = this.list(new LambdaQueryWrapper<Blog>().in(Blog::getId, ids)
                .last("ORDER BY FIELD(id," + idStr + ")"));

        for (Blog blog:blogs){
            queryUserByBlog(blog);
            isBlokLiked(blog);
        }
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setOffset(cnt);
        scrollResult.setMinTime(minTime);
        return Result.ok(scrollResult);
    }

    private void isBlokLiked(Blog blog){
       UserDTO userDTO = UserHolder.getUser();
       if(userDTO==null)
           return;// user do not login
        Long useId = userDTO.getId();
        String key = BLOG_LIKED_KEY+blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key,useId.toString());
        blog.setIsLike(score!=null);
    }
}
