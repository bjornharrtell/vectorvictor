package org.wololo.vectorvictor

import java.io._
import scala.collection.parallel._
import scalikejdbc._
import com.typesafe.scalalogging.LazyLogging
import scala.collection.mutable.ArrayBuffer

object tileops extends LazyLogging {
  def makeTiles(table: String, grid: Grid, zoom: Int): ArrayBuffer[Int] = {
    logger.debug("Making tiles for level " + zoom)

    val storedTiles = ArrayBuffer[Int]()

    // traverse grid and create a fetch/store function for each tile
    var c = 0
    val r = grid.range(zoom)
    val funcs = for (x <- r; y <- r) yield () => {
      val bytes = fetchTile(table, grid.tileExtent(x, y, zoom))
      if (!bytes.isEmpty) {
        storeTile(table, bytes.get, zoom, x, y)
        storedTiles += y * r.length + x
        c += 1
      }
    }

    // parallelize execution to use a pool of 20 threads
    val parFuncs = funcs.toParArray
    parFuncs.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(20))
    parFuncs.foreach(f => f())

    logger.debug("Tiles stored: " + c)

    storedTiles
  }

  def storeTile(name: String, tile: Array[Byte], zoom: Int, x: Int, y: Int) = {
    val path = s"output/${name}/${zoom}/${y}/"
    new File(path).mkdirs()
    val fos = new FileOutputStream(path + x)
    fos.write(tile, 0, tile.length)
    fos.close
  }

  def fetchTile(table: String, extent: Extent): Option[Array[Byte]] = DB.autoCommit { implicit session =>
    logger.debug(s"Fetching tile for " + extent)
    val envelope = sqls"ST_MakeEnvelope(${extent.minx}, ${extent.miny}, ${extent.maxx}, ${extent.maxy}, 3006)"
    val select = sqls"select gid, geom from ${SQLSyntax.createUnsafely(table)} left join t_vv on id = gid where geom @ ${envelope} and id is null"
    val bytes = sql"select ST_AsTWKB(array_agg(geom), array_agg(gid), -2) as geom from (${select}) as q".map(rs => rs.bytes(1)).single.apply()
    sql"insert into t_vv select gid from (${select}) as q".update.apply()
    bytes
  }
}