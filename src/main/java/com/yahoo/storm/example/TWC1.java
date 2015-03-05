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
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.CombinerAggregator;
import storm.trident.spout.IBatchSpout;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;

public class TWC1 {
  public static class RandomSentenceSpout implements IBatchSpout {
    private static final String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
          "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };

    int maxBatchSize = 3;
    int index = 0;
    Random _rand;
    HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();
    
    @Override
    public void open(Map conf, TopologyContext context) {
      index = 0;
      _rand = new Random();
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        List<List<Object>> batch = this.batches.get(batchId);
        if(batch == null){
            batch = new ArrayList<List<Object>>();
            if (index >= sentences.length) {
                index = 0;
            }
            for (int i=0; index < sentences.length && i < maxBatchSize; index++, i++) {
                String sentence = sentences[_rand.nextInt(sentences.length)];
                batch.add(new Values(sentence));
            }
            this.batches.put(batchId, batch);
        }
        for (List<Object> list : batch) {
            collector.emit(list);
        }
    }

    @Override
    public void ack(long batchId) {
        this.batches.remove(batchId);
    }

    @Override
    public void close() {
    }

    @Override
    public Map getComponentConfiguration() {
        return new Config();
    }

    @Override
    public Fields getOutputFields() {
      return new Fields("sentence");
    }
  }

  public static class Split extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String sentence = tuple.getString(0);
      for (String word : sentence.split("\\s+")) {
        collector.emit(new Values(word));
      }
    }
  }

  public static class Count implements CombinerAggregator<Long> {
    @Override
    public Long init(TridentTuple tuple) {
      return 1L;
    }

    @Override
    public Long combine(Long val1, Long val2) {
      return val1 + val2;
    }

    @Override
    public Long zero() {
      return 0L;
    }
  }

  public static void main(String[] args) throws Exception {
    RandomSentenceSpout spout = new RandomSentenceSpout();
    TridentTopology topology = new TridentTopology();
    TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(16).each(new Fields("sentence"),
        new Split(), new Fields("word")).groupBy(new Fields("word")).persistentAggregate(new MemoryMapState.Factory(),
        new Count(), new Fields("count")).parallelismHint(16);

    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    if (args.length == 0) {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("wordCounter", conf, topology.build());
      Thread.sleep(10000);
      cluster.shutdown();
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topology.build());
    }
  }
}
