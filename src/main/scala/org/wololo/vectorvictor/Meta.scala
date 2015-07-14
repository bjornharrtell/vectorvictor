package org.wololo.vectorvictor

case class SourceMeta(
  name: String,
  grid: Grid,
  maxZoom: Int,
  minResolution: Option[Int] = None,
  maxResolution: Option[Int] = None
)

case class CacheMeta(
  name: String,
  origin: (Int, Int),
  resolutions: Array[Int],
  minResolution: Option[Int] = None,
  maxResolution: Option[Int] = None,
  tiles: Array[Array[Int]]
)