package spout;
// standard library

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;

// storm-utils
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Created by hzcortex on 16-7-29.
 */
public class genRandomSentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector _collector;
    private Random _random;
    private int index_cnt = 0;


    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this._collector = collector;
        this._random = new Random();
    }


    public void nextTuple() {
        index_cnt++;
        Utils.sleep(1000); //
        String[] sentences = new String[]{"this",
                "is", "first"};
        String sentence = sentences[this._random.nextInt(sentences.length)];
        System.out.println("正在启动 ack 机制,已经发送了" + sentence);
        Object message_id = sentence + index_cnt;
        this._collector.emit(new Values(sentence), message_id);


    }


    public void ack(Object id) {
        // 可以使用 log4j 来看
        System.out.println("调用 ack 机制，成功处理了" + id);
    }


    public void fail(Object id) {
        System.out.println("调用 ack 机制，处理失败" + id + "正在重新发送");
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }


}
