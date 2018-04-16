package cn.xpleaf.bigdata.storm.statistic;

import cn.xpleaf.bigdata.storm.utils.JedisUtil;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 日志数据统计Bolt，实现功能：
 * 1.统计各省份的PV、UV
 * 2.以天为单位，将省份对应的PV、UV信息写入Redis
 */
public class StatisticBolt extends BaseBasicBolt {

    Map<String, Integer> pvMap = new HashMap<>();
    Map<String, HashSet<String>> midsMap = null;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (!input.getSourceComponent().equalsIgnoreCase(Constants.SYSTEM_COMPONENT_ID)) {  // 如果收到非系统级别的tuple，统计信息到局部变量mids
            String province = input.getStringByField("province");
            String mid = input.getStringByField("mid");
            pvMap.put(province, pvMap.get(province) + 1);   // pv+1
            if(mid != null) {
                midsMap.get(province).add(mid); // 将mid添加到该省份所对应的set中
            }
        } else {    // 如果收到系统级别的tuple，则将数据更新到Redis中，释放JVM堆内存空间
            /*
             * 以 广东 为例，其在Redis中保存的数据格式如下：
             * guangdong_pv（Redis数据结构为hash）
             *         --20180415
             *              --pv数
             *         --20180416
             *              --pv数
             * guangdong_mids_20180415(Redis数据结构为set)
             *         --mid
             *         --mid
             *         --mid
             *         ......
             * guangdong_mids_20180415(Redis数据结构为set)
             *         --mid
             *         --mid
             *         --mid
             *         ......
             */
            Jedis jedis = JedisUtil.getJedis();
            String dateStr = sdf.format(new Date());
            // 更新pvMap数据到Redis中
            String pvKey = null;
            for(String province : pvMap.keySet()) {
                int currentPv = pvMap.get(province);
                if(currentPv > 0) { // 当前map中的pv大于0才更新，否则没有意义
                    pvKey = province + "_pv";
                    String oldPvStr = jedis.hget(pvKey, dateStr);
                    if(oldPvStr == null) {
                        oldPvStr = "0";
                    }
                    Long oldPv = Long.valueOf(oldPvStr);
                    jedis.hset(pvKey, dateStr, oldPv + currentPv + "");
                    pvMap.replace(province, 0); // 将该省的pv重新设置为0
                }
            }
            // 更新midsMap到Redis中
            String midsKey = null;
            HashSet<String> midsSet = null;
            for(String province: midsMap.keySet()) {
                midsSet = midsMap.get(province);
                if(midsSet.size() > 0) {  // 当前省份的set的大小大于0才更新到，否则没有意义
                    midsKey = province + "_mids_" + dateStr;
                    jedis.sadd(midsKey, midsSet.toArray(new String[midsSet.size()]));
                    midsSet.clear();
                }
            }
            // 释放jedis资源
            JedisUtil.returnJedis(jedis);
            System.out.println(System.currentTimeMillis() + "------->写入数据到Redis");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    /**
     * 设置定时任务，只对当前bolt有效，系统会定时向StatisticBolt发送一个系统级别的tuple
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return config;
    }

    /**
     * 初始化各个省份的pv和mids信息（用来临时存储统计pv和uv需要的数据）
     */
    public StatisticBolt() {
        pvMap = new HashMap<>();
        midsMap = new HashMap<String, HashSet<String>>();
        String[] provinceArray = {"shanxi", "jilin", "hunan", "hainan", "xinjiang", "hubei", "zhejiang", "tianjin", "shanghai",
                "anhui", "guizhou", "fujian", "jiangsu", "heilongjiang", "aomen", "beijing", "shaanxi", "chongqing",
                "jiangxi", "guangxi", "gansu", "guangdong", "yunnan", "sicuan", "qinghai", "xianggang", "taiwan",
                "neimenggu", "henan", "shandong", "shanghai", "hebei", "liaoning", "xizang"};
        for(String province : provinceArray) {
            pvMap.put(province, 0);
            midsMap.put(province, new HashSet());
        }
    }
}
