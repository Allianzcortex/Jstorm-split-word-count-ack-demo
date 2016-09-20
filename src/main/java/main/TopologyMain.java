package main;

import backtype.storm.tuple.Fields;
import spout.genRandomSentenceSpout;
import bolt.splitSentenceBolt;
import bolt.wordCountBolt;

import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;


/**
 * Created by hzcortex on 16-7-29.
 */
public class TopologyMain {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("send",new genRandomSentenceSpout());

        builder.setBolt("split",new splitSentenceBolt()).shuffleGrouping("send");
        builder.setBolt("count",new wordCountBolt()).fieldsGrouping("split",new Fields("word"));

        Config conf=new Config();
        conf.setDebug(true);
        conf.setNumWorkers(5);
        conf.setNumAckers(3);
        //conf.setMaxTaskParallelism(3);
        LocalCluster cluster=new LocalCluster();
        cluster.submitTopology("word_count",conf,builder.createTopology());
        System.out.println("done--------------------------------------");
        //cluster.shutdown();
    }

}
