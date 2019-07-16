package com.aliware.core;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcStatus;
import org.apache.dubbo.rpc.cluster.loadbalance.AbstractLoadBalance;

import java.util.List;
import java.util.Random;

/**
 * @Author: WonderHeng
 * @CreateTime: 2019-07-16 15:41
 */
public class LeastActionLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "leastactive";
    private final Random random = new Random();

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // 总个数
        int length = invokers.size();
        // 最小的活跃数
        int leastActive = -1;
        // 相同最小活跃数的个数
        int leastCount = 0;
        // 相同最小活跃数的下标
        int [] leastIndexs = new int[length];
        // 总权重
        int totalWeight = 0;
        // 第一个权重，用于于计算是否相同
        int firstWeight = 0;
        // 是否所有权重相同
        boolean sameWeight = true;
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            // 活跃数
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()).getActive();
            // 权重
            int weight = invoker.getUrl().getMethodParameter(invocation.getMethodName(), Constants.WEIGHT_KEY, Constants.DEFAULT_WEIGHT);
            // 发现更小的活跃数，重新开始
            if (leastActive == -1 || active < leastActive) {
                // 记录最小活跃数
                leastActive = active;
                // 重新统计相同最小活跃数的个数
                leastCount = 1;
                // 重新记录最小活跃数下标
                leastIndexs[0] = i;
                // 重新累计总权重
                totalWeight = weight;
                // 记录第一个权重
                firstWeight = weight;
                // 还原权重相同标识
                sameWeight = true;
                // 累计相同最小的活跃数
            } else if (active == leastActive) {
                // 累计相同最小活跃数下标
                leastIndexs[leastCount++] = i;
                // 累计总权重
                totalWeight += weight;
                // 判断所有权重是否一样
                if (sameWeight && i > 0
                        && weight != firstWeight) {
                    sameWeight = false;
                }
            }
        }
        // assert(leastCount > 0)
        if (leastCount == 1) {
            // 如果只有一个最小则直接返回
            return invokers.get(leastIndexs[0]);
        }
        if (!sameWeight && totalWeight > 0) {
            // 如果权重不相同且权重大于0则按总权重数随机
            int offsetWeight = random.nextInt(totalWeight);
            // 并确定随机值落在哪个片断上
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexs[i];
                offsetWeight -= getWeight(invokers.get(leastIndex), invocation);
                if (offsetWeight < 0) {
                    return invokers.get(leastIndex);
                }
            }
        }
        // 如果权重相同或权重为0则均等随机
        return invokers.get(leastIndexs[random.nextInt(leastCount)]);
    }
}
