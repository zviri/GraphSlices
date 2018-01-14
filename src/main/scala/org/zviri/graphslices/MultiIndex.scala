package org.zviri.graphslices



class MultiIndex[A] private(val index: Map[Long, MultiIndex[A]], val _value: Option[A] = None) {

  def isLastLevel: Boolean = _value.nonEmpty

  def apply(key: Long): MultiIndex[A] = {
    index(key)
  }

  def value = _value

  def keys = index.keys

  def flatten: Seq[(Id, A)] = {
    def _collectValuesRec(multiIndex: MultiIndex[A], keyAccum: Id = Vector()): Seq[(Id, A)] = {
      if (multiIndex.isLastLevel) {
        Seq((keyAccum, multiIndex.value.get))
      } else {
        multiIndex.index.flatMap {
          case (key, value) => _collectValuesRec(value, keyAccum :+ key)
        }.toVector
      }
    }
    _collectValuesRec(this)
  }
}

object MultiIndex {

  def apply[A](keys: Seq[Id], values: Seq[A]): MultiIndex[A] = {
    apply((keys, values).zipped.toSeq)
  }

  def apply[A](items: Seq[(Id, A)]): MultiIndex[A] = {
    require(items.map(_._1).distinct.size == items.size, "VertexIds must be unique.")
    if (items.head._1.nonEmpty) {
      val indexMap = items.map {
        case (key, value) => (key.head, (key.drop(1), value))
      }.groupBy(_._1).mapValues {
        subValues => MultiIndex(subValues.map(_._2))
      }
      new MultiIndex(indexMap)
    } else {
      new MultiIndex(Map(), Some(items.head._2))
    }
  }
}

