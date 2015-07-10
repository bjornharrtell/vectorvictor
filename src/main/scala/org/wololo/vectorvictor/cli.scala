package org.wololo.vectorvictor

import com.typesafe.scalalogging.LazyLogging
import com.zaxxer.hikari._
import scalikejdbc._
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import tileops._

object cli extends App with LazyLogging {
  val config = new HikariConfig("hikari.properties")
  val ds = new HikariDataSource(config)

  ConnectionPool.singleton(new DataSourceConnectionPool(ds))

  val extent = Extent(-350000, 5600000, 1870000, 7980000)
  //val extent = Extent(200000, 6000000, 1000000, 7800000)
  val grid = Grid(extent)
  
  makeTileCaches()
  
  def makeTileCaches() {
    createTempTable()
    
    val sourceDefs = List(
      ("lantmateriet.al_riks", 4, 0, 2048),
      ("osm.land_polygons_z5_3006", 2, 128, 0),
      ("osm.land_polygons_z8_3006", 4, 128, 0)
    )
    
    val metas = sourceDefs.map(sourceDef => (makeTileCache _).tupled(sourceDef))
    
    dropTempTable()
    
    logger.info(toJSON(metas))
  }
  
  def toJSON(value: Any) : String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    //mapper.enable(SerializationFeature.INDENT_OUTPUT)
    mapper.writeValueAsString(value)
  }
  
  def makeTileCache(table: String, maxZoom: Int, minResolution: Int, maxResolution: Int) : Meta = {
    clearTempTable
    
    val zoomLevels = 0 to maxZoom
    val resolutions = zoomLevels.map(zoom => grid.resolution(zoom))
    
    logger.info(s"Making tilecache from ${table} from maxZoom ${maxZoom}")
    logger.info("Origin: " + grid.bounds.minx + ", " + grid.bounds.miny)
    logger.info("Resolutions: " + resolutions)
    
    val storedTiles = zoomLevels.reverse.map(zoom => makeTiles(table, grid, zoom))
    
    logger.info("Tiles created: " + storedTiles.map(a => a.size).sum)

    Meta(
      table,
      resolutions.toArray,
      if (minResolution != 0) minResolution else resolutions.head,
      if (maxResolution != 0) maxResolution else resolutions.last,
      storedTiles.map(tiles => tiles.toArray).toArray
    )
  }
}