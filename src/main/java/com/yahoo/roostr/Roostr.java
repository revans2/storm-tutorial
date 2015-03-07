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
package com.yahoo.roostr;

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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.io.Serializable;

/**
 * Roostr smart home hub.
 */
public class Roostr {
  private static final RoostrDB ROOSTR_DB = new RoostrDB();

  public static class UserLocationSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void nextTuple() {
      UserLoc ul = ROOSTR_DB.getNextUserLoc();
      if (ul != null) {
        System.out.println("USER\t"+ul.getUser()+"\t"+ul.getLoc());
        _collector.emit(new Values(ul.getUser(), ul.getLoc()));
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
      declarer.declare(new Fields("userId", "location"));
    }
  }

  public static class DistanceBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String userId = tuple.getString(0);
      Location loc = (Location)tuple.getValue(1);
      for (Map.Entry<String, Location> place: ROOSTR_DB.getUserPlaces(userId).entrySet()) {
        collector.emit(new Values(place.getKey(), userId, place.getValue().distance(loc)));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("placeId", "userId", "distance"));
    }
  }

  public static class OccupiedBolt extends BaseBasicBolt {
    Map<String, Map<String, Double>> _placeUserDistanceCache = new HashMap<String, Map<String, Double>>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String placeId = tuple.getString(0);
      String userId = tuple.getString(1);
      Double dist = tuple.getDouble(2);
      Map<String, Double> userDist = _placeUserDistanceCache.get(placeId);
      if (userDist == null) {
        userDist = new HashMap<String, Double>();
        _placeUserDistanceCache.put(placeId, userDist);
      }
      if (dist <= 20) {
        userDist.put(userId, dist);
      } else {
        userDist.remove(userId);
      }
      ROOSTR_DB.setOccupied(placeId, !userDist.isEmpty());
      if (userDist.isEmpty()) {
        _placeUserDistanceCache.remove(placeId);
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      //Empty
    }
  }

  public static void main(String[] args) throws Exception {
    Roost home = new Roost("home", new Location(0,0), ROOSTR_DB);
    Roost bobWork = new Roost("bobWork", new Location(100,100), ROOSTR_DB);
    Roost aliceWork = new Roost("aliceWork", new Location(10,30), ROOSTR_DB);
    
    User bob = new User("bob", 0, 0, ROOSTR_DB);
    User alice = new User("alice", 0, 0, ROOSTR_DB);
    home.addUser(bob);
    home.addUser(alice);
    aliceWork.addUser(alice);
    bobWork.addUser(bob);

    TopologyBuilder builder = new TopologyBuilder(); 
    builder.setSpout("loc", new UserLocationSpout(), 1);
    builder.setBolt("dist", new DistanceBolt(), 4).fieldsGrouping("loc", new Fields("userId"));
    builder.setBolt("occupied", new OccupiedBolt(), 4).fieldsGrouping("dist", new Fields("placeId"));

    Config conf = new Config();
    conf.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("roostrControl", conf, builder.createTopology());

    home.start();
    bobWork.start();
    aliceWork.start();
 
    Thread.sleep(5000);
    bob.move(10, 10);
    alice.move(10, 10);

    Thread.sleep(200);
    bob.move(20, 20);
    alice.move(10, 20);

    Thread.sleep(200);
    bob.move(30, 30);
    alice.move(10, 30);

    Thread.sleep(200);
    bob.move(40, 40);

    Thread.sleep(200);
    bob.move(50, 50);
 
    Thread.sleep(200);
    bob.move(60, 60);
 
    Thread.sleep(200);
    bob.move(70, 70);

    Thread.sleep(200);
    bob.move(80, 80);
 
    Thread.sleep(200);
    bob.move(90, 90);

    Thread.sleep(200);
    bob.move(100, 100);
 
    Thread.sleep(1000);
    bob.move(90, 90);
    alice.move(10, 20);

    Thread.sleep(200);
    bob.move(80, 80);
    alice.move(10, 10);

    Thread.sleep(200);
    bob.move(70, 70);
    alice.move(0, 0);

    Thread.sleep(200);
    bob.move(60, 60);

    Thread.sleep(200);
    bob.move(50, 50);

    Thread.sleep(200);
    bob.move(40, 40);

    Thread.sleep(200);
    bob.move(30, 30);

    Thread.sleep(200);
    bob.move(20, 20);

    Thread.sleep(200);
    bob.move(10, 10);

    Thread.sleep(200);
    bob.move(0, 0);

    Thread.sleep(1000);
    home.shutdown();
    bobWork.shutdown();
    aliceWork.shutdown();
    cluster.shutdown();
  }
}
