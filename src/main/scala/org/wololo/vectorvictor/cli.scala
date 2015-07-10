package org.wololo.vectorvictor

import com.typesafe.scalalogging.LazyLogging
import com.zaxxer.hikari._
import scalikejdbc._
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import js._
import tileops._

object cli extends App with LazyLogging {
  val config = new HikariConfig("hikari.properties")
  val ds = new HikariDataSource(config)

  ConnectionPool.singleton(new DataSourceConnectionPool(ds))

  val extent = Extent(-130000, 5800000, 1650000, 800000)
  //val extent = Extent(200000, 6000000, 1000000, 7800000)
  val grid = Grid(extent)

  makeTileCaches()
  
  def makeTileCaches() {
    val sourceDefs = List(
      ("lantmateriet.al_riks", 4),
      //("osm.land3", 6),
      ("osm.land_polygons_z8_subset_3006", 0)
    )
    
    val metas = sourceDefs.map(sourceDef => makeTileCache(sourceDef._1, sourceDef._2))
    logger.info(toJSON(metas))
  }
  
  def toJSON(value: Any) : String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.enable(SerializationFeature.INDENT_OUTPUT)
    mapper.writeValueAsString(value)
  }
  
  def makeTileCache(table: String, maxZoom: Int) : Meta = {
    // TODO: instead of assuming t_vv create unlogged table and drop on exit
    DB localTx { implicit session => sql"truncate t_vv".update.apply() }
    
    val zoomLevels = 0 to maxZoom
    val resolutions = zoomLevels.map(zoom => grid.resolution(zoom))
    
    logger.info(s"Making tilecache from ${table} from maxZoom ${maxZoom}")
    logger.info("Origin: " + grid.bounds.minx + ", " + grid.bounds.miny)
    logger.info("Resolutions: " + resolutions)
    
    val storedTiles = zoomLevels.reverse.map(zoom => makeTiles(table, grid, zoom))
    
    logger.info("Tiles created: " + storedTiles.map(a => a.size).sum)
    
    Meta(
      table, resolutions.toArray, resolutions.head, resolutions.last, storedTiles.map(tiles => tiles.toArray).toArray
    )
  }
}