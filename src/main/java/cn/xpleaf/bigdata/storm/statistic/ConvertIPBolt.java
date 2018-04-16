package cn.xpleaf.bigdata.storm.statistic;

import cn.xpleaf.bigdata.storm.utils.JedisUtil;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;

/**
 * 日志数据预处理Bolt，实现功能：
 *     1.提取实现业务需求所需要的信息：ip地址、客户端唯一标识mid
 *     2.查询IP地址所属地，并发送到下一个Bolt
 */
public class ConvertIPBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        byte[] binary = input.getBinary(0);
        String line = new String(binary);
        String[] fields = line.split("\t");

        if(fields == null || fields.length < 10) {
            return;
        }

        // 获取ip和mid
        String ip = fields[1];
        String mid = fields[2];

        // 根据ip获取其所属地（省份）
        String province = null;
        if (ip != null) {
            Jedis jedis = JedisUtil.getJedis();
            province = jedis.hget("ip_info_en", ip);
            // 需要释放jedis的资源，否则会报can not get resource from the pool
            JedisUtil.returnJedis(jedis);
        }

        // 发送数据到下一个bolt，只发送实现业务功能需要的province和mid
        collector.emit(new Values(province, mid));

    }

    /**
     * 定义了发送到下一个bolt的数据包含两个域：province和mid
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("province", "mid"));
    }
}
