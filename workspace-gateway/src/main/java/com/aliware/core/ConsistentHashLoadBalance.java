package com.aliware.core;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.loadbalance.AbstractLoadBalance;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @Author: WonderHeng
 * @CreateTime: 2019-07-16 13:53
 */
public class ConsistentHashLoadBalance extends AbstractLoadBalance {

    private final ConcurrentMap<String, ConsistentHashSelector<?>> selectors = new ConcurrentHashMap<String, ConsistentHashSelector<?>>();

    @SuppressWarnings("unchecked")
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // 获取调用方法名
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        // 生成调用列表hashCode
        int identityHashCode = System.identityHashCode(invokers);
        // 以调用方法名为key,获取一致性hash选择器
        ConsistentHashSelector<T> selector = (ConsistentHashSelector<T>) selectors.get(key);
        // 若不存在则创建新的选择器
        if (selector == null || selector.getIdentityHashCode() != identityHashCode) {
            // 创建ConsistentHashSelector时会生成所有虚拟结点
            selectors.put(key, new ConsistentHashSelector<T>(invokers, invocation.getMethodName(), identityHashCode));
            // 获取选择器
            selector = (ConsistentHashSelector<T>) selectors.get(key);
        }
        // 选择结点
        return selector.select(invocation);

    }

    private static final class ConsistentHashSelector<T> {
        // 虚拟结点
        private final TreeMap<Long, Invoker<T>> virtualInvokers;
        // 副本数
        private final int replicaNumber;
        // hashCode
        private final int identityHashCode;
        // 参数索引数组
        private final int[] argumentIndex;

        public ConsistentHashSelector(List<Invoker<T>> invokers, String methodName, int identityHashCode) {
            // 创建TreeMap 来保存结点
            this.virtualInvokers = new TreeMap<Long, Invoker<T>>();
            // 生成调用结点HashCode
            this.identityHashCode = System.identityHashCode(invokers);
            // 获取Url
            // dubbo://169.254.90.37:20880/service.DemoService?anyhost=true&application=srcAnalysisClient&check=false&dubbo=2.8.4&generic=false&interface=service.DemoService&loadbalance=consistenthash&methods=sayHello,retMap&pid=14648&sayHello.timeout=20000&side=consumer&timestamp=1493522325563
            URL url = invokers.get(0).getUrl();
            // 获取所配置的结点数，如没有设置则使用默认值160
            this.replicaNumber = url.getMethodParameter(methodName, "hash.nodes", 160);
            // 获取需要进行hash的参数数组索引，默认对第一个参数进行hash
            String[] index = Constants.COMMA_SPLIT_PATTERN.split(url.getMethodParameter(methodName, "hash.arguments", "0"));
            argumentIndex = new int[index.length];
            for (int i = 0; i < index.length; i++) {
                argumentIndex[i] = Integer.parseInt(index[i]);
            }
            // 创建虚拟结点
            // 对每个invoker生成replicaNumber个虚拟结点，并存放于TreeMap中
            for (Invoker<T> invoker : invokers) {

                for (int i = 0; i < replicaNumber / 4; i++) {
                    // 根据md5算法为每4个结点生成一个消息摘要，摘要长为16字节128位。
                    byte[] digest = md5(invoker.getUrl().toFullString() + i);
                    // 随后将128位分为4部分，0-31,32-63,64-95,95-128，并生成4个32位数，存于long中，long的高32位都为0
                    // 并作为虚拟结点的key。
                    for (int h = 0; h < 4; h++) {
                        long m = hash(digest, h);
                        virtualInvokers.put(m, invoker);
                    }
                }
            }
        }

        public int getIdentityHashCode() {

            return identityHashCode;

        }

        // 选择结点
        public Invoker<T> select(Invocation invocation) {
            // 根据调用参数来生成Key
            String key = toKey(invocation.getArguments());
            // 根据这个参数生成消息摘要
            byte[] digest = md5(key);
            //调用hash(digest, 0)，将消息摘要转换为hashCode，这里仅取0-31位来生成HashCode
            //调用sekectForKey方法选择结点。
            Invoker<T> invoker = sekectForKey(hash(digest, 0));
            return invoker;
        }

        private String toKey(Object[] args) {
            StringBuilder buf = new StringBuilder();
            for (int i : argumentIndex) {
                if (i >= 0 && i < args.length) {
                    buf.append(args[i]);
                }
            }
            return buf.toString();
        }

        //根据hashCode选择结点
        private Invoker<T> sekectForKey(long hash) {
            Invoker<T> invoker;
            Long key = hash;
            // 若HashCode直接与某个虚拟结点的key一样，则直接返回该结点
            if (!virtualInvokers.containsKey(key)) {
                // 若不一致，找到一个最小上届的key所对应的结点。
                SortedMap<Long, Invoker<T>> tailMap = virtualInvokers.tailMap(key);
                // 若存在则返回，例如hashCode落在图中[1]的位置
                // 若不存在，例如hashCode落在[2]的位置，那么选择treeMap中第一个结点
                // 使用TreeMap的firstKey方法，来选择最小上界。
                if (tailMap.isEmpty()) {
                    key = virtualInvokers.firstKey();
                } else {

                    key = tailMap.firstKey();
                }
            }
            invoker = virtualInvokers.get(key);
            return invoker;
        }

        private long hash(byte[] digest, int number) {
            return (((long) (digest[3 + number * 4] & 0xFF) << 24)
                    | ((long) (digest[2 + number * 4] & 0xFF) << 16)
                    | ((long) (digest[1 + number * 4] & 0xFF) << 8)
                    | (digest[0 + number * 4] & 0xFF))
                    & 0xFFFFFFFFL;
        }

        private byte[] md5(String value) {
            MessageDigest md5;
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            md5.reset();
            byte[] bytes = null;
            try {
                bytes = value.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            md5.update(bytes);
            return md5.digest();
        }
    }
}
