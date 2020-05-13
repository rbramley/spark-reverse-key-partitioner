/*
 * Copyright 2020 Robin Bramley
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbramley.spark.partition

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class ReverseKeyPartitionerSuite extends FunSuite  {

  test("Equality and inequality") {
    val p2 = new ReverseKeyPartitioner(2)
    val p4 = new ReverseKeyPartitioner(4)
    val anotherP4 = new ReverseKeyPartitioner(4)
    assert(p2 === p2)
    assert(p4 === p4)
    assert(p2 != p4)
    assert(p4 != p2)
    assert(p4 === anotherP4)
    assert(anotherP4 === p4)
    assert(!p2.equals(Some(3)))
  }

  test("hashCode is the partition count") {
    val p1 = new ReverseKeyPartitioner(1)
    assert(p1.hashCode === 1)
  }

  test("Partitions have to be > 0") {
    assertThrows[java.lang.IllegalArgumentException] { new ReverseKeyPartitioner(-1) }
    assertThrows[java.lang.IllegalArgumentException] { new ReverseKeyPartitioner(0) }
  }

  test("Mod results are positive") {
    val p1 = new ReverseKeyPartitioner(1)
    assert(p1.nonNegativeMod(1, 1) === 0)
    assert(p1.nonNegativeMod(-7, 2) === 1) // -7 % 2 = -1 + 2 = 1
  }

  test("ReverseKey partitioning - 10") {
    val rk10 = new ReverseKeyPartitioner(10)

    assert(rk10.getPartition("201") === 1)
    assert(rk10.getPartition("202") === 2)
    assert(rk10.getPartition("203") === 3)
    assert(rk10.getPartition("204") === 4)
    assert(rk10.getPartition("205") === 5)
    assert(rk10.getPartition("206") === 6)
    assert(rk10.getPartition("207") === 7)
    assert(rk10.getPartition("208") === 8)
    assert(rk10.getPartition("209") === 9)
    assert(rk10.getPartition("210") === 0)

    // edge case testing
    assert(rk10.getPartition(null) === 0)
    assert(rk10.getPartition(101L) === 1)
    assert(rk10.getPartition(201) === 1)
    assert(rk10.getPartition(-1) === 1)
    assert(rk10.getPartition(3) === 3)
    assert(rk10.getPartition((3,4)) === (3,4).hashCode.toString.takeRight(1).toInt)
    assert(rk10.getPartition("R") === "R".hashCode.toString.takeRight(1).toInt)
  }

  test("ReverseKey partitioning - 1000") {
    val rk1000 = new ReverseKeyPartitioner(1000)

    assert(rk1000.getPartition("201") === 102)
    assert(rk1000.getPartition("202") === 202)
    assert(rk1000.getPartition("203") === 302)
    assert(rk1000.getPartition("204") === 402)
    assert(rk1000.getPartition("205") === 502)

    assert(rk1000.getPartition("211") === 112)
    assert(rk1000.getPartition("212") === 212)
    assert(rk1000.getPartition("213") === 312)
    assert(rk1000.getPartition("214") === 412)
    assert(rk1000.getPartition("215") === 512)

    assert(rk1000.getPartition("1000") === 0)

    assert(rk1000.getPartition("1") === 1)
    assert(rk1000.getPartition("12") === 21)
    assert(rk1000.getPartition("10120") === 21)

    // check for consistency!
    assert(rk1000.getPartition("999") === rk1000.getPartition("999"))
  }
}
