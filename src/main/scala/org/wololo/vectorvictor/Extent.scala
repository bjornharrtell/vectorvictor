package org.wololo.vectorvictor

case class Extent(minx: Int, miny: Int, maxx: Int, maxy: Int) {
  def width = maxx - minx
  def height = maxy - miny
}
