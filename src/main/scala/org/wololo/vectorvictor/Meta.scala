package org.wololo.vectorvictor

case class Meta(
  name: String,
  resolutions: Array[Int],
  minResolution: Int,
  maxResolution: Int,
  tiles: Array[Array[Int]]
)