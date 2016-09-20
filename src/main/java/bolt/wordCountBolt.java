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

public class wordCountBolt implements IBasicBolt{
    private Map<String,Integer> counter=new HashMap<String, Integer>();


    public void prepare(Map map,TopologyContext context){
            map.clear();
    }



    public void execute(Tuple tuple,BasicOutputCollector collector){
        String word=tuple.getString(0);
        Integer count=counter.get(word);
        if(count==null)
            count=0;
        count+=1;
        counter.put(word,count);

        // display
        for(Map.Entry<String,Integer> entry:counter.entrySet()){
            System.out.println(entry.getKey()+' '+entry.getValue());
        }
        collector.emit(new Values(counter));


    }

    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("word-count"));
    }


    public void cleanup(){

    }

    public Map<String,Object> getComponentConfiguration() {
        return null;

    }
}
