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
 * A User/Smart phone.
 */
public class User {
  private String _id;
  private int _x;
  private int _y;
  private RoostrDB _db;

  public User(String id, int x, int y, RoostrDB db) {
    _id = id;
    _x = x;
    _y = y;
    _db = db;
    sendUpdate();
  }

  public String getId() {
    return _id;
  }

  private void sendUpdate() {
    try {
      _db.setUserLoc(_id, new Location(_x, _y));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void move(int x, int y) {
    _x = x;
    _y = y;
    sendUpdate();
  }
}
