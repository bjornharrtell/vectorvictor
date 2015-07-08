package org.wololo.vectorvictor

import java.io._
import com.zaxxer.hikari._
import scala.collection.parallel._
import scalikejdbc._
import com.typesafe.scalalogging.LazyLogging

case class Extent(minx: Int, miny: Int, maxx: Int, maxy: Int) {
  def width = maxx - minx
  def height = maxy - miny
}

case class Grid(extent: Extent) extends LazyLogging {
  logger.info("Initializing gridset from " + extent)
  
  val tileSize = 256
  
  val maxDistance = if (extent.width > extent.height) extent.width else extent.height
  val maxResolution = (math.pow(2, math.ceil(math.log(maxDistance) / math.log(2))) / tileSize).toInt
  logger.info("Calculated maxResolution " + maxResolution)
  
  val bounds = calcBounds()
  
  logger.info("TileGrid total bounds " + bounds)
  
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

object VectorVictor extends App with LazyLogging {
  val config = new HikariConfig("hikari.properties")
  val ds = new HikariDataSource(config)
  
  ConnectionPool.singleton(new DataSourceConnectionPool(ds))
 
  DB.autoCommit { implicit session => sql"truncate t_vv".update.apply() }
  
  val extent = Extent(200000, 6000000, 1000000, 7800000)
  
  val grid = Grid(extent)
  
  var resolutions = List[Int]()
  var zs = List[Int]()
  var z = 4
  //var bitmaps = List[List[Int]]()
    
  def makeTiles(level: Int) = {
    logger.info("Making tiles for level " + level)
    
    var bitmap = List[Int]()
    
    resolutions = resolutions :+ grid.resolution(level)
    //zs = zs :+ z
    
    var c = 0;
    val r = grid.range(level)
    val funcs = for (x <- r; y <- r) yield () => {
      val bytes = fetchTile(grid.tileExtent(x, y, level))
      if (!bytes.isEmpty) {
        storeTile(bytes.get, x, y)
        bitmap = bitmap :+ y * r.length + x
        c += 1
      }
    }
    
    val parFuncs = funcs.toParArray
    
    parFuncs.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(20))
    
    parFuncs.foreach(f => f())
    
    //bitmaps = bitmaps :+ bitmap
    
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
    
  def fetchTile(extent: Extent) : Option[Array[Byte]] = DB.autoCommit { implicit session =>
    logger.debug(s"Fetching tile for " + extent)
    val envelope = sqls"ST_MakeEnvelope(${extent.minx}, ${extent.miny}, ${extent.maxx}, ${extent.maxy}, 3006)"
    val select = sqls"select gid, geom from lantmateriet.my_riks left join t_vv on id = gid where geom @ ${envelope} and id is null"
    val bytes = sql"select ST_AsTWKB(array_agg(geom), array_agg(gid)) as geom from (${select}) as q".map(rs => rs.bytes(1)).single.apply()
    sql"insert into t_vv select gid from (${select}) as q".update.apply()
    bytes
  }
  
  makeTiles(8)
  makeTiles(6)
  makeTiles(4)
  makeTiles(2)
  makeTiles(0)
  
  // TODO: Output array representing "bitmap" of generated tiles
  //logger.info("Bitmaps: " + bitmaps.reverse)
      
  logger.info("Origin: " + grid.bounds.minx + ", " + grid.bounds.miny)
  logger.info("Resolutions: " + resolutions.reverse.toString)
}