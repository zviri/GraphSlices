package org.zviri.graphslices

import org.scalatest.{FlatSpec, Matchers}

class MultiIndexTest extends FlatSpec with Matchers {

  val testIndex = RecursiveMultiIndex(Vector(
    (Vector[Long](1, 2, 3), 1),
    (Vector[Long](1, 2, 4), 2),
    (Vector[Long](1, 3, 3), 3),
    (Vector[Long](1, 3, 4), 4),
    (Vector[Long](1, 4, 2), 5)
  ))

  it should "return all elements inserted" in {
    testIndex(1)(2)(3).value.get shouldEqual 1
    testIndex(1)(2)(4).value.get shouldEqual 2
    testIndex(1)(3)(3).value.get shouldEqual 3
    testIndex(1)(3)(4).value.get shouldEqual 4
    testIndex(1)(4)(2).value.get shouldEqual 5
  }

  it should "throw NoSuchElementException if it does not contain a key" in {
    a [NoSuchElementException] should be thrownBy {
      testIndex(1)(2)(2).value.get
    }
  }

  it should "throw IllegalArgumentException if the keys are not unique" in {
    a [IllegalArgumentException] should be thrownBy {
      RecursiveMultiIndex(Vector(
        (Vector[Long](1), 1),
        (Vector[Long](1), 2)
      ))
    }
  }

  "flatten" should "return a flattened Vector of all elements in the index" in {
    testIndex.flatten should contain theSameElementsAs  Vector(
      (Vector[Long](1, 2, 3), 1),
      (Vector[Long](1, 2, 4), 2),
      (Vector[Long](1, 3, 3), 3),
      (Vector[Long](1, 3, 4), 4),
      (Vector[Long](1, 4, 2), 5)
    )
  }

  "keys" should "return all keys in the top level of the index" in {
    testIndex.keys should contain theSameElementsAs  Vector(1)
  }

  "isLastLevel" should "return true if and only if the last level is being accessed" in {
    testIndex.isLastLevel shouldBe false
    testIndex(1).isLastLevel shouldBe false
    testIndex(1)(2).isLastLevel shouldBe false
    testIndex(1)(2)(3).isLastLevel shouldBe true
  }
}
