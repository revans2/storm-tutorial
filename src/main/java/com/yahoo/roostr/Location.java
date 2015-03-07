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

import java.io.Serializable;

/**
 * Location of something
 */
public class Location implements Serializable {
  private int _x;
  private int _y;
  public Location(int x, int y) {
    _x = x;
    _y = y;
  }

  public int getX(){ return _x;}
  public int getY(){ return _y;}

  public double distance(Location other) {
      double xdist = Math.abs(other.getX() - (double)_x);
      double ydist = Math.abs(other.getY() - (double)_y);
      return Math.sqrt((xdist * xdist) + (ydist * ydist));
  }

  public String toString() {
    return "[Location: "+_x+", "+_y+"]";
  }
}
