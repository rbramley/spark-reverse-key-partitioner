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

import scala.util.{Try, Success, Failure}
import org.apache.spark.Partitioner

/**
 * A org.apache.spark.Partitioner that implements reverse-key-based
 * partitioning.
 * @author Robin Bramley and original Spark contributors
 */
class ReverseKeyPartitioner(partitions: Int) extends Partitioner {
  require(partitions > 0, s"Number of partitions ($partitions) cannot be negative.")

  private val keyLength = (partitions - 1).toString.length

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => nonNegativeMod(getReverseKey(key), numPartitions)
  }

  // This is from org.apache.spark.util.Utils which is private[spark]
  private[partition] def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  private def getReverseKey(key: Any): Int = {
    val stringKey: String = key match {
      case k: String => k
      case l: Long => l.toString
      case i: Int => i.toString
      case _ => key.hashCode.toString
    }

    reverseKey(stringKey)
  }

  private def reverseKey(key: String): Int = {
    val rKey: String = key.takeRight(keyLength).reverse
    Try(rKey.toInt) match {
      case Success(x) => x
      case Failure(_) => reverseKey(rKey.hashCode.toString)
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: ReverseKeyPartitioner =>
      r.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
