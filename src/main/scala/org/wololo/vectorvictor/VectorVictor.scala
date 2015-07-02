package org.wololo.vectorvictor

import java.io._
import com.zaxxer.hikari._
import resource._
import scala.collection.parallel.mutable.ParArray
import com.typesafe.scalalogging.slf4j.LazyLogging

case class Extent(minx:Int, miny: Int, maxx: Int, maxy: Int) {
  def width = maxx - minx
  def height = maxy - miny
  def rx = Range(minx, maxx)
  def rt = Range(miny, maxy)
  def toBBOX = List(minx, miny, maxx, maxy).mkString(",")
}

object VectorVictor extends App with LazyLogging {
  val config = new HikariConfig("hikari.properties")
  val ds = new HikariDataSource(config)
  
  val extent = Extent(218128, 6126002, 1083427, 7692850)
  
  val size = 108162.3242375
  //val size = 13520.2905296875
  
  val gridExtent = Extent(0, 0, Math.ceil(extent.width / size).toInt - 1, Math.ceil(extent.height / size).toInt - 1)
  
  def calcTileExtent(x:Int, y:Int) : Extent = {
    var minx = extent.minx + Math.ceil(size * x).toInt
    var miny = extent.miny + Math.ceil(size * y).toInt
    var maxx = minx + size.toInt
    var maxy = miny + size.toInt
    Extent(minx, miny, maxx, maxy)
  }
  
  def makeTiles() = {
    var funcs = for (
        x <- gridExtent.minx to gridExtent.maxx; y <- gridExtent.miny to gridExtent.maxy)
      yield () => fetchTile(x, y)
    
    funcs.toParArray.foreach(f => f())
  }
  
  def storeTile(tile: Array[Byte], x: Int, y: Int) = {
    val path = s"output/${y}/"
    new File(path).mkdir
    val fos = new FileOutputStream(path + x)
    fos.write(tile, 0, tile.length)
    fos.close
  }
  
  def fetchTile(x: Int, y: Int) {
    logger.info(s"Fetching tile ${x}, ${y}")
    
    val tileExtent = calcTileExtent(x, y);
    val envelope = s"ST_MakeEnvelope(${tileExtent.toBBOX}, 3006)";
    var sql = s"select ST_AsTWKB(array_agg(geom), array_agg(gid)) as geom from lantmateriet.ak_riks where geom && ${envelope}";

    var connection: java.sql.Connection = null
    var statement: java.sql.Statement = null
    var resultSet: java.sql.ResultSet = null
    try {
      connection = ds.getConnection
      statement = connection.createStatement()
      resultSet = statement.executeQuery(sql)
      resultSet.next
      val bytes = resultSet.getBytes(1)
      if (bytes != null) storeTile(bytes, x, y)
    } finally {
      resultSet.close()
      statement.close()
      connection.close()
    }
  }
  
  makeTiles
}