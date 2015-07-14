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

  makeTileCaches()
  
  ds.close()
  
  def makeTileCaches() {
    val grid1 = Grid(Extent(-350000, 5600000, 1870000, 7980000))
    val grid2 = Grid(Extent(200000, 6000000, 1000000, 7800000))
    
    createTempTable()
    
    val sourceMetas = List(
      SourceMeta("osm.land_polygons_z5_3006", grid1, 2, Some(1024), None),
      SourceMeta("osm.land_polygons_z8_3006", grid1, 4, Some(128), Some(1024)),
      SourceMeta("osm.land_polygons_3006", grid2, 8, None, Some(128)),
      SourceMeta("lantmateriet.al_riks", grid2, 4, None, Some(256))
    )
    
    val cacheMetas = sourceMetas.map(sourceMeta => makeTileCache(sourceMeta))
    
    dropTempTable()
    
    logger.info(toJSON(cacheMetas))
  }
  
  def toJSON(value: Any) : String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    //mapper.enable(SerializationFeature.INDENT_OUTPUT)
    mapper.writeValueAsString(value)
  }
  
  def makeTileCache(sourceMeta: SourceMeta) : CacheMeta = {
    clearTempTable
    
    val zoomLevels = 0 to sourceMeta.maxZoom
    val resolutions = zoomLevels.map(zoom => sourceMeta.grid.resolution(zoom))
    
    logger.info(s"Making tilecache from ${sourceMeta.name} from maxZoom ${sourceMeta.maxZoom}")
    //logger.info("Origin: " + sourceMeta.grid.bounds.minx + ", " + sourceMeta.grid.bounds.miny)
    //logger.info("Resolutions: " + resolutions)
    
    val storedTiles = zoomLevels.reverse.map(zoom => makeTiles(sourceMeta.name, sourceMeta.grid, zoom))
    
    logger.info("Tiles created: " + storedTiles.map(a => a.size).sum)

    CacheMeta(
      sourceMeta.name,
      sourceMeta.grid.origin,
      resolutions.toArray,
      sourceMeta.minResolution,
      sourceMeta.maxResolution,
      storedTiles.map(tiles => tiles.toArray).toArray
    )
  }
}