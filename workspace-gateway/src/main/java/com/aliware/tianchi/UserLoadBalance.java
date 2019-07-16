package com.aliware.tianchi;

import com.aliware.core.ConsistentHashLoadBalance;
import com.aliware.core.LeastActionLoadBalance;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author daofeng.xjf
 * <p>
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
/*
public class UserLoadBalance implements LoadBalance {

     随机路由
     * @Override
     * public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }
}*/

/**
 * 一致性hash
 */
public class UserLoadBalance implements LoadBalance {

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
//        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
        /*一致性hash*/
//        ConsistentHashLoadBalance consistentHashLoadBalance = new ConsistentHashLoadBalance();
//        return consistentHashLoadBalance.select(invokers, url, invocation);

        LeastActionLoadBalance leastActionLoadBalance = new LeastActionLoadBalance();
        return leastActionLoadBalance.select(invokers, url, invocation);
    }
}


/**
 * 权值计算
 */

//public class UserLoadBalance implements LoadBalance {
//    private volatile List<Double> accumulatedWeights = new ArrayList<>();
//    private int INIT = 0;
//
//    /**
//     *
//     * @param accumulatedWeights
//     */
//    private void setAccumulatedWeights(List<Double> accumulatedWeights) {
//        this.accumulatedWeights = accumulatedWeights;
//    }
//
//    private void init() {
//        double weightSoFar = 0.0;
//        List<Double> finalWeights = new ArrayList<>();
//        for (int i = 0; i < 3; i++) {
//            weightSoFar += 0.01;
//            finalWeights.add(weightSoFar);
//        }
//        setAccumulatedWeights(finalWeights);
//    }
//
//    @Override
//    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
//        if (INIT == 0) {
//            INIT++;
//            init();
//        }
//        // ORDER-1
//        if (invokers == null) {
//            return null;
//        }
//        Invoker<T> finalInvoker = null;
//
//        while (finalInvoker == null) {
//            List<Double> currentWeights = accumulatedWeights;
//            if (Thread.interrupted()) {
//                return null;
//            }
//
//            int invokerCnt = invokers.size();
//
//            if (invokerCnt == 0) {
//                return null;
//            }
//
//            int invokerInx = 0;
//
//            double maxTotalWeight = currentWeights.size() == 0 ? 0 : currentWeights.get(currentWeights.size() - 1);
//            if (maxTotalWeight < 0.001d || invokerCnt != currentWeights.size()) {
//                return invokers.get(2);
//            } else {
//                double randomWeight = ThreadLocalRandom.current().nextDouble() * maxTotalWeight;
//                int n = 0;
//                for (Double d : currentWeights) {
//                    if (d >= randomWeight) {
//                        invokerInx = n;
//                        break;
//                    } else {
//                        n++;
//                    }
//                }
//                System.out.println(invokerInx);
//                finalInvoker = invokers.get(invokerInx);
//            }
//
//            if (finalInvoker == null) {
//                Thread.yield();
//                continue;
//            }
//
//            if (finalInvoker.isAvailable()) {
//                System.out.println(finalInvoker.toString());
//                return finalInvoker;
//            }
//
//            finalInvoker = null;
//        }
//        return finalInvoker;
//    }
//}
