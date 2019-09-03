package com.sjyttkl.learning;

import java.util.BitSet;

/**
 * Create with: com.sjyttkl.learning
 * author: sjyttkl
 * E-mail: 695492835@qq.com
 * date: 2019/9/3 20:13
 * version: 1.0
 * description:如何在十亿个单词中，判断某个单词是否存在？布隆过滤器  实现布隆过滤器
 *   https://mbd.baidu.com/newspage/data/landingshare?pageType=1&isBdboxFrom=1&context=%7B%22nid%22:%22news_8898161154561857399%22,%22sourceFrom%22:%22bjh%22%7D&_refluxos=i3
 */
public class MyBloomFilter {
//    2 << 25 表示32位比特位
    private static final int DEFAULT_SIZE = 2<<25;
    private static final int[] seeds = new int []  {3,5,7,11,13,19,23,37};//八个不同的随机数产生器
    //这么大的存储在BitSet
    private BitSet bits = new BitSet(DEFAULT_SIZE);
    private SimpleHash[] func = new SimpleHash[seeds.length];

    //构造函数
    public MyBloomFilter(){
        for (int i = 0 ;i<seeds.length;i++){
            func[i] = new SimpleHash(DEFAULT_SIZE,seeds[i]);
        }
    }
    public static void main(String args[]){
        //可疑网站
        String value = "www.java323.com";
        MyBloomFilter filter = new MyBloomFilter();
        //加入之前的判断
        System.out.println(filter.contains(value));
        filter.add(value);
        //加入之后再继续判断一下
        System.out.println(filter.contains(value));
    }


    //添加网站
    public void add(String value){
        for (SimpleHash f:func){
            bits.set(f.hash(value),true);
        }
    }
    //判断是否有可疑网站
    public boolean contains(String value){
        if (value == null){
            return false;
        }
        boolean ret = true;
        for (SimpleHash f:func){
            //核心就是通过“与”操作 第一个 为false 的时候，后面的将不会允许。
            ret = ret && bits.get(f.hash(value));
        }
        return ret;

    }
    //SimpleHash
    public static class SimpleHash{
        private int cap;
        private  int seed;

        /**
         * 构造函数
         * @param cap 建立一个32亿二进制（比特），静态的
         * @param seed 随机数种子
         */
        public SimpleHash(int cap, int seed ){
            this.cap = cap;
            this.seed = seed;
        }
        public int hash(String value){
            int result = 0;
            int len = value.length();
            for(int i = 0 ;i<len;i++){
                result = seed * result +value.charAt(i);

            }
            return (cap - 1 ) & result;
        }
    }
}

