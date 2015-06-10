package com.tataatsu;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.tataatsu.spout.RandomSentenceSpout;

import java.util.HashMap;
import java.util.Map;

//* Created by gggopi on 9/6/15.


public class WordCountTopology {
    public static class SplitSentence extends ShellBolt implements IRichBolt {

        public SplitSentence() {

            super("python", "splitsentences.py");
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
    public static class WordCount extends BaseBasicBolt{

        Map<String, Integer> counts = new HashMap<String, Integer>();

        //@Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            System.out.println(word+ " " + count);
            counts.put(word, count);
            System.out.println(counts.toString());
            //collector.emit(new Values(word, count));
        }

        //@Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }
    public static void main(String args[]) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomSentenceSpout(), 1);

        builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 1).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(1);
            conf.setNumWorkers(1);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount", conf, builder.createTopology());

            Thread.sleep(1000000);

            cluster.shutdown();
        }
    }
}
