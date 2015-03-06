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
package com.yahoo.temp;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Bang Bang temperature controller
 */
public class BangBang {
  //This is a bit of a hack to allow feedback in local mode
  private static Map<String, Integer> heaterOn = new ConcurrentHashMap<String, Integer>();
  public static void setHeaterOn(String heater, int amount) {
    if (amount < 0) amount = 0;
    if (amount > 10) amount = 10;
    heaterOn.put(heater, amount);
  }

  public static class TemperatureSpout extends BaseRichSpout {
    Map<String, Integer> temperatures = new HashMap<String, Integer>();
    SpoutOutputCollector _collector;
    Random _rand;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
      _rand = new Random();
    }

    @Override
    public void nextTuple() {
      try { Thread.sleep(100); } catch (Exception e) {}
      for (Map.Entry<String, Integer> entry: heaterOn.entrySet()) {
        Integer temp = temperatures.get(entry.getKey());
        Integer amount = entry.getValue();
        //TODO This is really dumb make it more real...
        if (temp == null) temp = _rand.nextInt(100);
        if (amount > 0) {
          temp += _rand.nextInt(amount);
        } else {
          temp -= _rand.nextInt(3);
        }
        temperatures.put(entry.getKey(), temp);
        System.out.println("RESULT\t"+entry.getKey()+"\t"+temp+"\t"+entry.getValue());
        _collector.emit(new Values(entry.getKey(), temp, System.currentTimeMillis()));
      }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("name", "temperature", "time"));
    }
  }

  public static class BangBangBolt extends BaseBasicBolt {
    int targetTemp;

    public BangBangBolt(int targetTemp) {
        this.targetTemp = targetTemp;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String name = tuple.getString(0);
      Integer temp = tuple.getInteger(1);
      Long time = tuple.getLong(2);
      if (temp < targetTemp) {
          setHeaterOn(name, 10);
      } else if (temp > targetTemp) {
          setHeaterOn(name, 0);
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //Empty
    }
  }

  public static void main(String[] args) throws Exception {
    setHeaterOn("living", 1);
    setHeaterOn("office", 0);
    setHeaterOn("den", 1);
    setHeaterOn("reactorA", 10);
    setHeaterOn("reactorB", 0);

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new TemperatureSpout(), 1);
    builder.setBolt("controller", new BangBangBolt(72), 4).fieldsGrouping("spout", new Fields("name"));

    Config conf = new Config();

    conf.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", conf, builder.createTopology());

    Thread.sleep(10000);

    cluster.shutdown();
  }
}
