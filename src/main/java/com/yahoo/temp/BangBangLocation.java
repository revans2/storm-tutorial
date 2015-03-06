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
import java.io.Serializable;

/**
 * Bang Bang temperature controller, with user location
 */
public class BangBangLocation {
  //This is a bit of a hack to allow feedback in local mode
  private static Map<String, Integer> heaterOn = new ConcurrentHashMap<String, Integer>();
  public static void setHeaterOn(String heater, int amount) {
    if (amount < 0) amount = 0;
    if (amount > 10) amount = 10;
    heaterOn.put(heater, amount);
  }

  private static class Location implements Serializable {
    private int x;
    private int y;
    public Location(int x, int y) {
      this.x = x;
      this.y = y;
    }

    public int getX(){ return x;}
    public int getY(){ return y;}

    public double distance(Location other) {
        double xdist = Math.abs(other.getX() - (double)x);
        double ydist = Math.abs(other.getY() - (double)y);
        return Math.sqrt((xdist * xdist) + (ydist * ydist));
    }

    public String toString() {
        return "[Location: "+x+", "+y+"]";
    }
  }

  private static Map<String, Location> location = new ConcurrentHashMap<String, Location>();
  public static void setLocation(String place, Location loc) {
    location.put(place, loc);
  }

  public static class LocationSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    int _x = 0;
    int _y = 0;
    Random _rand;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
      _rand = new Random();
    }

    @Override
    public void nextTuple() {
      try { Thread.sleep(100); } catch (Exception e) {}
      Location loc = new Location(_x, _y);
      System.out.println("USER\tuser\t"+loc);
      _collector.emit(new Values("user", loc, System.currentTimeMillis()));

      if (_x < 100) {
        _x += _rand.nextInt(5);
        if (_x > 100) _x = 100;
      }

      if (_y < 100) {
        _y += _rand.nextInt(5);
        if (_y > 100) _y = 100;
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
      declarer.declare(new Fields("user", "location", "time"));
    }
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
          temp += _rand.nextInt(amount + 1);
        } else {
          temp -= _rand.nextInt(4);
        }
        temperatures.put(entry.getKey(), temp);
        System.out.println("TEMP\t"+entry.getKey()+"\t"+temp+"\t"+entry.getValue());
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

  public static class DistanceBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String name = tuple.getString(0);
      Location loc = (Location)tuple.getValue(1);
      Long time = tuple.getLong(2);
      for (Map.Entry<String, Location> place: location.entrySet()) {
        collector.emit(new Values(place.getKey(), name, place.getValue().distance(loc), time));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("place", "user", "distance", "time"));
    }
  }


  public static class BangBangBolt extends BaseBasicBolt {
    int targetTemp;
    int targetEmptyTemp;

    public BangBangBolt(int targetTemp, int targetEmptyTemp) {
        this.targetTemp = targetTemp;
        this.targetEmptyTemp = targetEmptyTemp;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      if (tuple.contains("place")) {

      } else {
        String name = tuple.getString(0);
        Integer temp = tuple.getInteger(1);
        Long time = tuple.getLong(2);
        if (temp < targetTemp) {
          setHeaterOn(name, 10);
        } else if (temp > targetTemp) {
          setHeaterOn(name, 0);
        }
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //Empty
    }
  }

  public static void main(String[] args) throws Exception {
    setLocation("place-a", new Location(0, 0));
    setLocation("place-b", new Location(10, 30));
    setLocation("place-c", new Location(100, 100));

    setHeaterOn("place-a", 1);
    setHeaterOn("place-b", 0);
    setHeaterOn("place-c", 1);

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("loc", new LocationSpout(), 1);
    builder.setBolt("dist", new DistanceBolt(), 4).shuffleGrouping("loc");

    builder.setSpout("temp", new TemperatureSpout(), 1);
    builder.setBolt("controller", new BangBangBolt(72, 50), 4)
        .fieldsGrouping("temp", new Fields("name"))
        .fieldsGrouping("dist", new Fields("place"));

    Config conf = new Config();

    conf.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", conf, builder.createTopology());

    Thread.sleep(10000);

    cluster.shutdown();
  }
}
