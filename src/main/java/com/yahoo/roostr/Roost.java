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

import java.util.Random;

/**
 * Represents the smart home hub
 */
public class Roost extends Thread {
  private int _temp = 50;
  private int _heaterAmount = 0;
  private int _occupiedTemp = 72;
  private int _emptyTemp = 52;
  private boolean _isOccupied = false;
  private Random _rand = new Random();
  private String _id;
  private volatile boolean _done = false;
  private RoostrDB _db;

  public Roost(String id, Location loc, RoostrDB db) {
    _id = id;
    _db = db;
    _db.registerPlace(id, loc);
  }

  public void addUser(User user) {
    _db.addUserToPlace(_id, user.getId());
  }

  public void shutdown() throws InterruptedException {
    _done = true;
    this.join();
  }

  protected void simulationStep() {
    //a crummy almost random simulation
    if (_heaterAmount > 0) {
      _temp += _rand.nextInt(_heaterAmount + 1);
    } else {
      _temp -= _rand.nextInt(4);
    }
  }

  protected void runController() {
    //A simple bang bang controller
    int targetTemp = _isOccupied ? _occupiedTemp : _emptyTemp;
    if (_temp < targetTemp) {
      _heaterAmount = 10;
    } else if (_temp > targetTemp) {
      _heaterAmount = 0;
    }
  }

  protected void reportAndUpdateState() {
    _isOccupied = _db.isOccupied(_id);
  }

  public void run() {
    while (!_done) {
      simulationStep();
      runController();
      reportAndUpdateState();
      System.out.println("ROOST: "+_id+" "+_temp+" "+_isOccupied);
      try { Thread.sleep(100); } catch(Exception e){throw new RuntimeException(e);}
    }
  }
}
