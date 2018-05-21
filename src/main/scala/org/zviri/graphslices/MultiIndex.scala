package org.zviri.graphslices


trait MultiIndex[A] {

  def isLastLevel: Boolean

  def apply(key: Long): MultiIndex[A]

  def value: Option[A]

  def keys: Iterable[Long]

  def flatten: Seq[(Id, A)]

}

class RecursiveMultiIndex[A] private(val index: Map[Long, RecursiveMultiIndex[A]], val _value: Option[A] = None) extends MultiIndex[A] {

  def isLastLevel: Boolean = _value.nonEmpty

  def apply(key: Long): MultiIndex[A] = {
    index(key)
  }

  def value = _value

  def keys = index.keys

  def flatten: Seq[(Id, A)] = {
    def _collectValuesRec(multiIndex: RecursiveMultiIndex[A], keyAccum: Id = Vector()): Seq[(Id, A)] = {
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

object RecursiveMultiIndex {

  def apply[A](keys: Seq[Id], values: Seq[A]): MultiIndex[A] = {
    apply((keys, values).zipped.toSeq)
  }

  def apply[A](items: Seq[(Id, A)]): RecursiveMultiIndex[A] = {
    require(items.map(_._1).distinct.size == items.size, "Ids must be unique.")
    if (items.head._1.nonEmpty) {
      val indexMap = items.map {
        case (key, value) => (key.head, (key.drop(1), value))
      }.groupBy(_._1).mapValues {
        subValues => RecursiveMultiIndex(subValues.map(_._2))
      }
      new RecursiveMultiIndex[A](indexMap)
    } else {
      new RecursiveMultiIndex[A](Map(), Some(items.head._2))
    }
  }
}

class FlatMultiIndex[A] private(val _index: Seq[(Id, A)]) extends MultiIndex[A] {

  def isLastLevel: Boolean = _index.head._1.size == 0

  def apply(key: Long): MultiIndex[A] = {
    val newIndex = _index.filter{
      case (id, _) => id.head == key
    }.map{
      case (id, v) => (id.drop(1), v)
    }
    if (newIndex.isEmpty) throw new NoSuchElementException(s"Key $key not found in the index!")

    new FlatMultiIndex(newIndex)
  }

  def value = if (_index.isEmpty) None else Some(_index.head._2)

  def keys = _index.map{ case (id, _) => id.head }.distinct

  def flatten: Seq[(Id, A)] = {
    _index
  }
}

object FlatMultiIndex {

  def apply[A](keys: Seq[Id], values: Seq[A]): MultiIndex[A] = {
    apply((keys, values).zipped.toSeq)
  }

  def apply[A](items: Seq[(Id, A)]): FlatMultiIndex[A] = {
    require(items.map(_._1).distinct.size == items.size, "Ids must be unique.")
    new FlatMultiIndex[A](items)
  }
}
