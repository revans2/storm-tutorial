/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.storm.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Word Count topology over a window of time, outputting results after we a period of time, and on late data.
 * and skewed
 */
public class WC5 {
  public static class RandomSentenceSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
      _rand = new Random();
    }

    @Override
    public void nextTuple() {
      try { Thread.sleep(10); } catch (Exception e) {}
      String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
          "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature",
          "a a a a a a a a a a a a a a a a a a a a a a a a a a a a", "a a a a a a a a a a a a a a a a a a a a a"};
      String sentence = sentences[_rand.nextInt(sentences.length)];
      long now = System.currentTimeMillis();
      if (_rand.nextInt(100) >= 99) {
        //Late Data
        now = now - 2000;
      }
      _collector.emit(new Values(sentence, now));
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("sentence", "time"));
    }
  }

  public static class SplitSentence extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String sentence = tuple.getString(0);
      Long time = tuple.getLong(1);
      for (String word: sentence.split("\\s+")) {
        collector.emit(new Values(word, time));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "time"));
    }
  }

  public static class WindowWordCount extends BaseRichBolt {
    private Map<Long, Map<String, Integer>> counts = new HashMap<Long, Map<String, Integer>>();
    private OutputCollector collector;
    private long lateBucket = 0;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      String word = tuple.getString(0);
      Long time = tuple.getLong(1);
      long bucket = time/1000;
      long now = System.currentTimeMillis();
      long currentBucket = now/1000;
      long tooOld = currentBucket-20;
      if (bucket <= tooOld) {
          collector.fail(tuple);
      } else {
        Map<String, Integer> byWord = counts.get(bucket);
        if (byWord == null) {
          byWord = new HashMap<String, Integer>();
          counts.put(bucket, byWord);
        }
        Integer count = byWord.get(word);
        if (count == null) {
          count = 0;
        }
        count++;
        byWord.put(word, count);

        if (bucket < lateBucket) {
          System.out.println("PARTIAL UPDATE RESULT: "+word+" "+bucket+" "+count);
          collector.emit(tuple, new Values(word, bucket, count));
        }
        collector.ack(tuple);
      }
      if ((currentBucket - 1) != lateBucket) {
        //We rolled over
        Map<String, Integer> byWord = counts.get(lateBucket);
        if (byWord != null) {
          for (Map.Entry<String, Integer> entry: byWord.entrySet()) {
            System.out.println("PARTIAL RESULT: "+entry.getKey()+" "+lateBucket+" "+entry.getValue());
            collector.emit(new Values(entry.getKey(), lateBucket, entry.getValue()));
          }
        }
        lateBucket = currentBucket - 1;
      }
      counts.remove(tooOld);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "bucket", "count"));
    }
  }

  public static class WindowWordCountJoin extends BaseRichBolt {
    private Map<Long, Map<String, Map<Integer, Integer>>> counts = new HashMap<Long, Map<String, Map<Integer, Integer>>>();
    private OutputCollector collector;
    private long lateBucket = 0;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      String word = tuple.getString(0);
      Long bucket = tuple.getLong(1);
      Integer count = tuple.getInteger(2);
      int from = tuple.getSourceTask();
     
      long now = System.currentTimeMillis();
      long currentBucket = now/1000;
      long tooOld = currentBucket-20;

      if (bucket <= tooOld) {
          //really should never happen
          collector.fail(tuple);
      } else {
        Map<String, Map<Integer, Integer>> byWord = counts.get(bucket);
        if (byWord == null) {
          byWord = new HashMap<String, Map<Integer, Integer>>();
          counts.put(bucket, byWord);
        }
        Map<Integer, Integer> byUpstream = byWord.get(word);
        if (byUpstream == null) {
          byUpstream = new HashMap<Integer, Integer>();
          byWord.put(word, byUpstream);
        }
        byUpstream.put(from, count);

        if (bucket < lateBucket) {
          int total = 0;
          for (Integer partial: byUpstream.values()) {
            total += partial;
          }
          System.out.println("UPDATE RESULT: "+word+" "+bucket+" "+total);
          collector.emit(tuple, new Values(word, bucket, total));
        }
        collector.ack(tuple);
      }
      if ((currentBucket - 2) != lateBucket) {
        //We rolled over
        Map<String, Map<Integer, Integer>> byWord = counts.get(lateBucket);
        if (byWord != null) {
          for (Map.Entry<String, Map<Integer, Integer>> entry: byWord.entrySet()) {
            int total = 0;
            for (Integer partial: entry.getValue().values()) {
              total += partial;
            }
            System.out.println("RESULT: "+entry.getKey()+" "+lateBucket+" "+total);
            collector.emit(new Values(entry.getKey(), lateBucket, total));
          }
        }
        lateBucket = currentBucket - 2;
      }
      counts.remove(tooOld);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "bucket", "count"));
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new RandomSentenceSpout(), 5);

    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
    //builder.setBolt("count", new WindowWordCount(), 12).partialKeyGrouping("split", new Fields("word"));
    builder.setBolt("count", new WindowWordCount(), 12).customGrouping("split", new PartialKeyGrouping(new Fields("word")));
    builder.setBolt("join", new WindowWordCountJoin(), 3).fieldsGrouping("count", new Fields("word"));

    Config conf = new Config();

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }
  }
}
