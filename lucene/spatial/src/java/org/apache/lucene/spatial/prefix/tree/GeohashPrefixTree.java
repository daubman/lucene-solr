package org.apache.lucene.spatial.prefix.tree;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.io.GeohashUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * A {@link SpatialPrefixTree} based on
 * <a href="http://en.wikipedia.org/wiki/Geohash">Geohashes</a>.
 * Uses {@link GeohashUtils} to do all the geohash work.
 *
 * @lucene.experimental
 */
public class GeohashPrefixTree extends SpatialPrefixTree {

  /**
   * Factory for creating {@link GeohashPrefixTree} instances with useful defaults
   */
  public static class Factory extends SpatialPrefixTreeFactory {

    @Override
    protected int getLevelForDistance(double degrees) {
      GeohashPrefixTree grid = new GeohashPrefixTree(ctx, GeohashPrefixTree.getMaxLevelsPossible());
      return grid.getLevelForDistance(degrees);
    }

    @Override
    protected SpatialPrefixTree newSPT() {
      return new GeohashPrefixTree(ctx,
          maxLevels != null ? maxLevels : GeohashPrefixTree.getMaxLevelsPossible());
    }
  }

  public GeohashPrefixTree(SpatialContext ctx, int maxLevels) {
    super(ctx, maxLevels);
    Rectangle bounds = ctx.getWorldBounds();
    if (bounds.getMinX() != -180)
      throw new IllegalArgumentException("Geohash only supports lat-lon world bounds. Got "+bounds);
    int MAXP = getMaxLevelsPossible();
    if (maxLevels <= 0 || maxLevels > MAXP)
      throw new IllegalArgumentException("maxLen must be [1-"+MAXP+"] but got "+ maxLevels);
  }

  /** Any more than this and there's no point (double lat & lon are the same). */
  public static int getMaxLevelsPossible() {
    return GeohashUtils.MAX_PRECISION;
  }

  @Override
  public int getLevelForDistance(double dist) {
    if (dist == 0)
      return maxLevels;//short circuit
    final int level = GeohashUtils.lookupHashLenForWidthHeight(dist, dist);
    return Math.max(Math.min(level, maxLevels), 1);
  }

  @Override
  public Cell getCell(Point p, int level) {
    return new GhCell(GeohashUtils.encodeLatLon(p.getY(), p.getX(), level));//args are lat,lon (y,x)
  }

  @Override
  public Cell getCell(byte[] bytes, int offset, int len) {
    return new GhCell(bytes, offset, len);
  }

  private static byte[] stringToBytesPlus1(String token) {
    //copy ASCII token to byte array with one extra spot for eventual LEAF_BYTE if needed
    byte[] bytes = new byte[token.length() + 1];
    for (int i = 0; i < token.length(); i++) {
      bytes[i] = (byte) token.charAt(i);
    }
    return bytes;
  }

  class GhCell extends Cell {

    private Shape shape;//cache
    private String geohash;//cache; never has leaf byte, simply a geohash

    GhCell(String geohash) {
      super(stringToBytesPlus1(geohash), 0, geohash.length());
      this.geohash = geohash;
      if (isLeaf())
        this.geohash = geohash.substring(0, geohash.length() - 1);
    }

    GhCell(byte[] bytes, int off, int len) {
      super(bytes, off, len);
    }

    @Override
    protected SpatialPrefixTree getGrid() { return GeohashPrefixTree.this; }

    @Override
    public void reset(byte[] bytes, int off, int len) {
      super.reset(bytes, off, len);
      geohash = null;
      shape = null;
    }

    @Override
    public Collection<Cell> getSubCells() {
      String[] hashes = GeohashUtils.getSubGeohashes(getGeohash());//sorted
      List<Cell> cells = new ArrayList<>(hashes.length);
      for (String hash : hashes) {
        cells.add(new GhCell(hash));
      }
      return cells;
    }

    @Override
    public int getSubCellsSize() {
      return 32;//8x4
    }

    @Override
    public Cell getSubCell(Point p) {
      return getGrid().getCell(p, getLevel() + 1);//not performant!
    }

    @Override
    public Shape getShape() {
      if (shape == null) {
        shape = GeohashUtils.decodeBoundary(getGeohash(), getGrid().getSpatialContext());
      }
      return shape;
    }

    @Override
    public Point getCenter() {
      return GeohashUtils.decode(getGeohash(), getGrid().getSpatialContext());
    }

    private String getGeohash() {
      if (geohash == null)
        geohash = getTokenBytesNoLeaf(null).utf8ToString();
      return geohash;
    }

  }//class GhCell

}
