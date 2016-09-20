package bolt;

// standard-import
import java.util.Map;
import java.util.HashMap;

// storm
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;

/**
 * Created by hzcortex on 16-7-29.
 */
public class splitSentenceBolt implements IBasicBolt{

    //@Override
    public void prepare(Map map,TopologyContext context){

    }

    //@Override
    public void execute(Tuple tuple,BasicOutputCollector collector){
        String sentence=tuple.getString(0);
        for(String word:sentence.split(" ")){
            collector.emit(new Values(word));
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("word"));
    }
    public void cleanup(){

    }

    public Map<String,Object> getComponentConfiguration(){
        return null;

    }

}
