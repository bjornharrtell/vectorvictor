package org.wololo.vectorvictor

import com.typesafe.scalalogging.LazyLogging
import com.zaxxer.hikari._
import scalikejdbc._

import js._
import tileops._

object cli extends App with LazyLogging {
  val config = new HikariConfig("hikari.properties")
  val ds = new HikariDataSource(config)

  ConnectionPool.singleton(new DataSourceConnectionPool(ds))

  val extent = Extent(200000, 6000000, 1000000, 7800000)
  val grid = Grid(extent)
  
  makeTileCache("lantmateriet.al_riks", 4)
  makeTileCache("osm.land3", 6)
  
  def makeTileCache(table: String, maxZoom: Int) {
    // TODO: instead of assuming t_vv create unlogged table and drop on exit
    DB localTx { implicit session => sql"truncate t_vv".update.apply() }
    
    val zoomLevels = 0 to maxZoom
    val resolutions = zoomLevels.map(zoom => grid.resolution(zoom))
    
    logger.info(s"Making tilecache from ${table} from maxZoom ${maxZoom}")
    logger.info("Origin: " + grid.bounds.minx + ", " + grid.bounds.miny)
    logger.info("Resolutions: " + resolutions)
    
    val storedTiles = zoomLevels.reverse.map(zoom => makeTiles(table, grid, zoom))
    
    logger.info("Tiles created: " + storedTiles.map(a => a.size).sum)
    
    // TODO: output JS stuff
    // toJS(storedTiles)
  }
}