package org.wololo.vectorvictor

import java.io._
import com.zaxxer.hikari._
import resource._
import scala.collection.parallel._
import com.typesafe.scalalogging.LazyLogging

case class Extent(minx: Int, miny: Int, maxx: Int, maxy: Int) {
  def width = maxx - minx
  def height = maxy - miny
  def toBBOX = List(minx, miny, maxx, maxy).mkString(",")
}

case class Grid(extent: Extent) extends LazyLogging {
  logger.info("Initializing gridset from " + extent)
  
  val tileSize = 256
  
  val maxDistance = if (extent.width > extent.height) extent.width else extent.height
  val maxResolution = (math.pow(2, math.ceil(math.log(maxDistance) / math.log(2))) / tileSize).toInt
  logger.info("Calculated maxResolution " + maxResolution)
  
  val bounds = Extent(
      (Math.ceil(extent.minx / maxResolution) * maxResolution).toInt,
      (Math.ceil(extent.miny / maxResolution) * maxResolution).toInt,
      (Math.ceil(extent.minx / maxResolution) * maxResolution + tileSize * maxResolution).toInt,
      (Math.ceil(extent.miny / maxResolution) * maxResolution + tileSize * maxResolution).toInt
  )
  
  logger.info("TileGrid total bounds " + bounds)
  
  def resolution(level: Int) = (maxResolution/math.pow(2, level)).toInt
  def range(level: Int) = 0 to math.pow(2, level).toInt-1
  
  def tileExtent(x:Int, y:Int, level: Int) : Extent = {
    val w = tileSize * resolution(level)
    val minx = bounds.minx + w * x
    val miny = extent.miny + w * y
    Extent(minx, miny, minx + w, miny + w)
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
  
  val grid = Grid(extent)
  
  var resolutions = List[Int]()
  var zs = List[Int]()
  var z = 2 
  
  def makeTiles(level: Int) = {
    logger.info("Making tiles for level " + level)
    
    resolutions = resolutions :+ grid.resolution(level)
    zs = zs :+ z
    
    var c = 0;
    val r = grid.range(level)
    val funcs = for (x <- r; y <- r) yield () => {
      val bytes = fetchTile(grid.tileExtent(x, y, level).toBBOX)
      if (bytes != null) {
        storeTile(bytes, x, y)
        c += 1
      }
    }
    
    val parFuncs = funcs.toParArray
    
    parFuncs.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(20))
    
    parFuncs.foreach(f => f())
    
    z -= 1
    
    logger.info("Tiles stored: " + c)
  }
  
  def storeTile(tile: Array[Byte], x: Int, y: Int) = {
    val path = s"output/${z}/${y}/"
    new File(path).mkdirs()
    val fos = new FileOutputStream(path + x)
    fos.write(tile, 0, tile.length)
    fos.close
  }
  
  def fetchTile(bbox: String) : Array[Byte] = {
    logger.debug(s"Fetching tile for " + bbox)
    
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
  
  makeTiles(4)
  makeTiles(2)
  makeTiles(0)
  
  logger.info("Origin: " + grid.bounds.minx + ", " + grid.bounds.miny)
  logger.info("Resolutions: " + resolutions.reverse.toString)
}