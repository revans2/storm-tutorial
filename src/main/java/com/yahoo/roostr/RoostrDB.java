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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an interface to a centralized DB and message queue.
 */
public class RoostrDB {
  private Map<String, Location> _places = new HashMap<String, Location>();
  private Map<String, Set<String>> _usersPlaces = new HashMap<String, Set<String>>();
  private Map<String, Boolean> _placeOccupied = new HashMap<String, Boolean>();
  private LinkedBlockingQueue<UserLoc> _userLocQueue = new LinkedBlockingQueue<UserLoc>();

  public synchronized void registerPlace(String id, Location loc) {
    _places.put(id, loc);
  }

  public synchronized void addUserToPlace(String placeId, String userId) {
    Set<String> places = _usersPlaces.get(userId);
    if (places == null) {
      places = new HashSet<String>();
      _usersPlaces.put(userId, places);
    }
    places.add(placeId);
  }

  public synchronized Map<String, Location> getUserPlaces(String userId) {
    HashMap<String, Location> ret = new HashMap<String, Location>();
    Set<String> places = _usersPlaces.get(userId);
    if (places != null) {
      for (String placeId: places) {
        Location loc = _places.get(placeId);
        if (loc != null) {
          ret.put(placeId, loc);
        }
      }
    }
    return ret;
  }

  public synchronized boolean isOccupied(String placeId) {
    Boolean found = _placeOccupied.get(placeId);
    return found == null || found;
  }

  public synchronized void setOccupied(String placeId, boolean occ) {
    _placeOccupied.put(placeId, occ);
  }

  public UserLoc getNextUserLoc() {
    return _userLocQueue.poll();
  }

  public void setUserLoc(String user, Location loc) throws InterruptedException {
    _userLocQueue.put(new UserLoc(user, loc));
  }
}
