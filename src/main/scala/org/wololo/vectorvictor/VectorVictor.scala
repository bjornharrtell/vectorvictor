package org.wololo.vectorvictor

import java.io._
import com.zaxxer.hikari._
import resource._
import scala.collection.parallel._
import com.typesafe.scalalogging.LazyLogging

case class Extent(val minx: Double, miny: Double, maxx: Double, maxy: Double) {
  def width = maxx - minx
  def height = maxy - miny
  def toBBOX = List(minx, miny, maxx, maxy).mkString(",")
}

// How to find out max resolution from extent:
// Math.ceil(Math.log2(distance))
// How to select an extent that is divisable with resolution
// Math.ceil(origo/resolution)*resolution


case class GridExtent(extent: Extent, level: Int) {
  val tileSize = extent.height / math.pow(2, level)
  //val tileSize = 256
  val minx = 0
  val miny = 0
  val maxx = Math.ceil(extent.width / tileSize).toInt - 1
  val maxy = Math.ceil(extent.height / tileSize).toInt - 1
  def rx = minx to maxx
  def ry = miny to maxy
  def tileExtent(x:Int, y:Int) : Extent = {
    val minx = extent.minx + (tileSize * x)
    val miny = extent.miny + (tileSize * y)
    Extent(minx, miny, minx + tileSize, miny + tileSize)
  }
}

object VectorVictor extends App with LazyLogging {
  val config = new HikariConfig("hikari.properties")
  val ds = new HikariDataSource(config)
  
  var connection: java.sql.Connection = null
  var statement: java.sql.Statement = null
 
  try {
    connection = ds.getConnection
    statement = connection.createStatement()
    statement.executeUpdate("truncate t_vv")
  } finally {
    statement.close()
    connection.close()
  }
  
  val extent = Extent(218128, 6126002, 1083427, 7692850)
  
  val levels = Array(
    GridExtent(extent, 0),
    GridExtent(extent, 1),
    GridExtent(extent, 2),
    GridExtent(extent, 3),
    GridExtent(extent, 4),
    GridExtent(extent, 5),
    GridExtent(extent, 6)
  )
  
  def makeTiles(level: Int) = {
    // TODO: should fetch tiles for increasing levels, only fetching objects that fit in bbox
    // TODO: remember every object that has been fetched (where?!)
    // TODO: only fetch not already fetched objects
    // TODO: uhu.. that requies sequential processing right? no, only level to level
    
    var grid = levels(level)
    
    logger.info(s"Using tilesize " + grid.tileSize)
    
    var funcs = for (x <- grid.rx; y <- grid.ry) yield () => {
      val bytes = fetchTile(grid.tileExtent(x,y).toBBOX)
      if (bytes != null) storeTile(bytes, x, y, level)
    }
    
    val parFuncs = funcs.toParArray
    
    parFuncs.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(20))
    
    parFuncs.foreach(f => f())
  }
  
  def storeTile(tile: Array[Byte], x: Int, y: Int, z: Int) = {
    val path = s"output/${z}/${y}/"
    new File(path).mkdirs()
    val fos = new FileOutputStream(path + x)
    fos.write(tile, 0, tile.length)
    fos.close
  }
  
  def fetchTile(bbox: String) : Array[Byte] = {
    logger.info(s"Fetching tile for " + bbox)
    
    val envelope = s"ST_MakeEnvelope(${bbox}, 3006)";
    var insert = s"insert into t_vv select gid from lantmateriet.ak_riks left join t_vv on gid = id where geom @ ${envelope} and id is null";
    var select = s"select ST_AsTWKB(array_agg(geom), array_agg(gid)) as geom from (select gid, geom from lantmateriet.ak_riks left join t_vv on id = gid where geom @ ${envelope} and id is null) as q";
    //var select = s"select ST_AsTWKB(array_agg(geom), array_agg(gid)) as geom from lantmateriet.ak_riks where geom @ ${envelope}";

    var result: Array[Byte] = null
    
    var connection: java.sql.Connection = null
    var statement: java.sql.Statement = null
    var resultSet: java.sql.ResultSet = null
 
    try {
      connection = ds.getConnection
      statement = connection.createStatement()
      resultSet = statement.executeQuery(select)
      resultSet.next
      result = resultSet.getBytes(1)
    } finally {
      if (resultSet != null) resultSet.close()
      statement.close()
      connection.close()
    }
    
    try {
      connection = ds.getConnection
      statement = connection.createStatement()
      statement.executeUpdate(insert)
      result
    } finally {
      statement.close()
      connection.close()
    }
  }
  
  makeTiles(6)
  makeTiles(5)
  makeTiles(4)
  makeTiles(3)
  makeTiles(2)
  makeTiles(1)
  makeTiles(0)
}