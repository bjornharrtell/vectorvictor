package org.wololo.vectorvictor

import com.typesafe.scalalogging.LazyLogging

case class Grid(extent: Extent) extends LazyLogging {
  logger.info("Initializing grid from " + extent)
  
  val tileSize = 256
  
  val maxDistance = if (extent.width > extent.height) extent.width else extent.height
  val maxResolution = (math.pow(2, math.ceil(math.log(maxDistance) / math.log(2))) / tileSize).toInt
  logger.info("Calculated maxResolution " + maxResolution)
  
  val bounds = calcBounds()
  
  logger.info("TileGrid total bounds " + bounds)
  
  def origin = (bounds.minx, bounds.miny)
  def resolution(level: Int) = (maxResolution / math.pow(2, level)).toInt
  def range(level: Int) = 0 to math.pow(2, level).toInt - 1
  
  def tileExtent(x: Int, y: Int, level: Int) : Extent = {
    val w = tileSize * resolution(level)
    val minx = bounds.minx + w * x
    val miny = bounds.miny + w * y
    Extent(minx, miny, minx + w, miny + w)
  }
  
  def calcBounds() : Extent = {
    var minx = Math.ceil(extent.minx / maxResolution) * maxResolution
    var miny = Math.ceil(extent.miny / maxResolution) * maxResolution
    var maxx = minx + tileSize * maxResolution
    var maxy = miny + tileSize * maxResolution
    Extent(minx.toInt, miny.toInt, maxx.toInt, maxy.toInt)
  }
}
