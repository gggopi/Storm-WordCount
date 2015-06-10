package com.tataatsu.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

/*
 * Created by gggopi on 9/6/15.
 */
public class RandomSentenceSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;
    int count = 0;
    //String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away", "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
    List<String> sentences = new ArrayList<String>(Arrays.asList("the cow jumped over the moon", "an apple a day keeps the doctor away", "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature"));

    //@Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

   // @Override
    public void nextTuple() {
        if(count == sentences.size()){
            return;
        }
        else {
            for(String sentence : sentences){
                System.out.println(sentence);
                _collector.emit(new Values(sentence));
                count++;
            }
        }
        Utils.sleep(100);
    }

    @Override
    public void ack(Object id) {
    }


    @Override
    public void fail(Object id) {

    }

    //@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}
