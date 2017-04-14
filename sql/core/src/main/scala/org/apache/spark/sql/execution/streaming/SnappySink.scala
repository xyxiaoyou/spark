/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package org.apache.spark.sql.execution.streaming

import javax.annotation.concurrent.GuardedBy

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/** A sink that stores the results in SnappyData store.
  * This [[org.apache.spark.sql.execution.streaming.Sink]] is in memory only as of now.
  */

class SnappySink(val schema: StructType, outputMode: OutputMode)
  extends Sink with Logging {

  private case class AddedData(batchId: Long, data: Array[Row])

  /** An order list of batches that have been written to this [[Sink]]. */
  @GuardedBy("this")
  private val batches = new ArrayBuffer[AddedData]()

  /** Returns all rows that are stored in this [[Sink]]. */
  def allData: Seq[Row] = synchronized {
    batches.map(_.data).flatten
  }

  def latestBatchId: Option[Long] = synchronized {
    batches.lastOption.map(_.batchId)
  }

  def latestBatchData: Seq[Row] = synchronized {
    batches.lastOption.toSeq.flatten(_.data)
  }

  def toDebugString: String = synchronized {
    batches.map { case AddedData(batchId, data) =>
      val dataStr = try data.mkString(" ") catch {
        case NonFatal(e) => "[Error converting to string]"
      }
      s"$batchId: $dataStr"
    }.mkString("\n")
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val notCommitted = synchronized {
      latestBatchId.isEmpty || batchId > latestBatchId.get
    }
    if (notCommitted) {
      logDebug(s"Committing batch $batchId to $this")
      outputMode match {
        case InternalOutputModes.Append | InternalOutputModes.Update =>
          val rows = AddedData(batchId, data.collect())
          synchronized {
            batches += rows
          }

        case InternalOutputModes.Complete =>
          val rows = AddedData(batchId, data.collect())
          synchronized {
            batches.clear()
            batches += rows
          }

        case _ =>
          throw new IllegalArgumentException(
            s"Output mode $outputMode is not supported by SnappySink")
      }
    } else {
      logDebug(s"Skipping already committed batch: $batchId")
    }
  }

  def clear(): Unit = synchronized {
    batches.clear()
  }

  override def toString(): String = "SnappySink"

}

/**
  * Used to query the data that has been written into a [[SnappySink]].
  */
case class SnappyPlan(sink: SnappySink, output: Seq[Attribute]) extends LeafNode {
  def this(sink: SnappySink) = this(sink, sink.schema.toAttributes)

  private val sizePerRow = sink.schema.toAttributes.map(_.dataType.defaultSize).sum

  override def statistics: Statistics = Statistics(sizePerRow * sink.allData.size)
}

class SnappySinkProvider extends StreamSinkProvider with DataSourceRegister {
  def createSink(
                  df: DataFrame,
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): Sink = {
    val sink = new SnappySink(df.schema, outputMode)
    val resultDf = Dataset.ofRows(df.sparkSession, new SnappyPlan(sink))
    resultDf.createOrReplaceTempView(parameters.get("queryName").get)
    sink
  }

  def shortName(): String = "snappy"
}
