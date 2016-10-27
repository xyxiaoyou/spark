/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import java.util.{ArrayList, Collections}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.storage.{BlockId, BlockStatus, StorageLevel}
import org.apache.spark.util.{AccumulatorContext, AccumulatorMetadata, AccumulatorV2, AccumulatorV2Kryo, DoubleAccumulator, LongAccumulator}


/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.
 *
 * This class is wrapper around a collection of internal accumulators that represent metrics
 * associated with a task. The local values of these accumulators are sent from the executor
 * to the driver when the task completes. These values are then merged into the corresponding
 * accumulator previously registered on the driver.
 *
 * The accumulator updates are also sent to the driver periodically (on executor heartbeat)
 * and when the task failed with an exception. The [[TaskMetrics]] object itself should never
 * be sent to the driver.
 */
@DeveloperApi
class TaskMetrics private[spark] () extends Serializable with KryoSerializable {
  // Each metric is internally represented as an accumulator
  private val _executorDeserializeTime = new DoubleAccumulator
  private val _executorRunTime = new DoubleAccumulator
  private val _resultSize = new LongAccumulator
  private val _jvmGCTime = new LongAccumulator
  private val _resultSerializationTime = new DoubleAccumulator
  private val _memoryBytesSpilled = new LongAccumulator
  private val _diskBytesSpilled = new LongAccumulator
  private val _peakExecutionMemory = new LongAccumulator
  private val _updatedBlockStatuses = new BlockStatusesAccumulator

  /**
   * Time taken on the executor to deserialize this task.
   */
  def executorDeserializeTime: Long = _executorDeserializeTime.sum.toLong

  /**
   * Time the executor spends actually running the task (including fetching shuffle data).
   */
  def executorRunTime: Long = _executorRunTime.sum.toLong

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult.
   */
  def resultSize: Long = _resultSize.sum

  /**
   * Amount of time the JVM spent in garbage collection while executing this task.
   */
  def jvmGCTime: Long = _jvmGCTime.sum

  /**
   * Amount of time spent serializing the task result.
   */
  def resultSerializationTime: Long = _resultSerializationTime.sum.toLong

  /**
   * The number of in-memory bytes spilled by this task.
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled.sum

  /**
   * The number of on-disk bytes spilled by this task.
   */
  def diskBytesSpilled: Long = _diskBytesSpilled.sum

  /**
   * Peak memory used by internal data structures created during shuffles, aggregations and
   * joins. The value of this accumulator should be approximately the sum of the peak sizes
   * across all such data structures created in this task. For SQL jobs, this only tracks all
   * unsafe operators and ExternalSort.
   */
  def peakExecutionMemory: Long = _peakExecutionMemory.sum

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   */
  def updatedBlockStatuses: Seq[(BlockId, BlockStatus)] = {
    // This is called on driver. All accumulator updates have a fixed value. So it's safe to use
    // `asScala` which accesses the internal values using `java.util.Iterator`.
    _updatedBlockStatuses.value.asScala
  }

  // Setters and increment-ers
  private[spark] def setExecutorDeserializeTime(v: Long): Unit =
    _executorDeserializeTime.setValue(v)
  private[spark] def setExecutorDeserializeTime(v: Double): Unit =
    _executorDeserializeTime.setValue(v)
  private[spark] def setExecutorRunTime(v: Long): Unit = _executorRunTime.setValue(v)
  private[spark] def setExecutorRunTime(v: Double): Unit = _executorRunTime.setValue(v)
  private[spark] def setResultSize(v: Long): Unit = _resultSize.setValue(v)
  private[spark] def setJvmGCTime(v: Long): Unit = _jvmGCTime.setValue(v)
  private[spark] def setResultSerializationTime(v: Long): Unit =
    _resultSerializationTime.setValue(v)
  private[spark] def resultSerializationTimeMetric = _resultSerializationTime
  private[spark] def incMemoryBytesSpilled(v: Long): Unit = _memoryBytesSpilled.add(v)
  private[spark] def incDiskBytesSpilled(v: Long): Unit = _diskBytesSpilled.add(v)
  private[spark] def incPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.add(v)
  private[spark] def incUpdatedBlockStatuses(v: (BlockId, BlockStatus)): Unit =
    _updatedBlockStatuses.add(v)
  private[spark] def setUpdatedBlockStatuses(v: java.util.List[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v)
  private[spark] def setUpdatedBlockStatuses(v: Seq[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v.asJava)

  /**
   * Metrics related to reading data from a [[org.apache.spark.rdd.HadoopRDD]] or from persisted
   * data, defined only in tasks with input.
   */
  val inputMetrics: InputMetrics = new InputMetrics()

  /**
   * Metrics related to writing data externally (e.g. to a distributed filesystem),
   * defined only in tasks with output.
   */
  val outputMetrics: OutputMetrics = new OutputMetrics()

  /**
   * Metrics related to shuffle read aggregated across all shuffle dependencies.
   * This is defined only if there are shuffle dependencies in this task.
   */
  val shuffleReadMetrics: ShuffleReadMetrics = new ShuffleReadMetrics()

  /**
   * Metrics related to shuffle write, defined only in shuffle map stages.
   */
  val shuffleWriteMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics()

  /**
   * A list of [[TempShuffleReadMetrics]], one per shuffle dependency.
   *
   * A task may have multiple shuffle readers for multiple dependencies. To avoid synchronization
   * issues from readers in different threads, in-progress tasks use a [[TempShuffleReadMetrics]]
   * for each dependency and merge these metrics before reporting them to the driver.
   */
  @transient private lazy val tempShuffleReadMetrics = new ArrayBuffer[TempShuffleReadMetrics]

  /**
   * Create a [[TempShuffleReadMetrics]] for a particular shuffle dependency.
   *
   * All usages are expected to be followed by a call to [[mergeShuffleReadMetrics]], which
   * merges the temporary values synchronously. Otherwise, all temporary data collected will
   * be lost.
   */
  private[spark] def createTempShuffleReadMetrics(): TempShuffleReadMetrics = synchronized {
    val readMetrics = new TempShuffleReadMetrics
    tempShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Merge values across all temporary [[ShuffleReadMetrics]] into `_shuffleReadMetrics`.
   * This is expected to be called on executor heartbeat and at the end of a task.
   */
  private[spark] def mergeShuffleReadMetrics(): Unit = synchronized {
    if (tempShuffleReadMetrics.nonEmpty) {
      shuffleReadMetrics.setMergeValues(tempShuffleReadMetrics)
    }
  }

  // Only used for test
  private[spark] var testAccum = if (TaskMetrics.sparkTesting) Some(new LongAccumulator) else None


  private val _internalAccums = {
    val accums = Array(
      _executorDeserializeTime,
      _executorRunTime,
      _resultSize,
      _jvmGCTime,
      _resultSerializationTime,
      _memoryBytesSpilled,
      _diskBytesSpilled,
      _peakExecutionMemory,
      _updatedBlockStatuses,
      shuffleReadMetrics._remoteBlocksFetched,
      shuffleReadMetrics._localBlocksFetched,
      shuffleReadMetrics._remoteBytesRead,
      shuffleReadMetrics._localBytesRead,
      shuffleReadMetrics._fetchWaitTime,
      shuffleReadMetrics._recordsRead,
      shuffleWriteMetrics._bytesWritten,
      shuffleWriteMetrics._recordsWritten,
      shuffleWriteMetrics._writeTime,
      inputMetrics._bytesRead,
      inputMetrics._recordsRead,
      outputMetrics._bytesWritten,
      outputMetrics._recordsWritten
    )
    val len = accums.length
    var i = 0
    while (i < len) {
      accums(i).internalId = (i + 1).toByte
      i += 1
    }
    testAccum match {
      case None => accums
      case Some(acc) => accums :+ acc
    }
  }

  private[spark] def internalAccums: Seq[AccumulatorV2[_, _]] = _internalAccums.toSeq

  /* ========================== *
   |        OTHER THINGS        |
   * ========================== */

  private[spark] def register(sc: SparkContext): Unit = {
    val names = TaskMetrics.internalAccumsNames
    val accums = _internalAccums
    for (i <- names.indices) {
      val name = names(i)
      val acc = accums(i)
      acc.register(sc, name = Some(name), countFailedValues = true)
    }
  }

  /**
   * External accumulators registered with this task.
   */
  @transient private[spark] lazy val externalAccums = new ArrayBuffer[AccumulatorV2[_, _]](4)

  private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    externalAccums += a
  }

  private[spark] def accumulators(): Seq[AccumulatorV2[_, _]] = _internalAccums ++ externalAccums

  /**
   * Looks for a registered accumulator by accumulator name.
   */
  private[spark] def lookForAccumulatorByName(name: String): Option[AccumulatorV2[_, _]] = {
    accumulators.find { acc =>
      acc.name.isDefined && acc.name.get == name
    }
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    _executorDeserializeTime.write(kryo, output)
    _executorRunTime.write(kryo, output)
    _resultSize.write(kryo, output)
    _jvmGCTime.write(kryo, output)
    _resultSerializationTime.write(kryo, output)
    _memoryBytesSpilled.write(kryo, output)
    _diskBytesSpilled.write(kryo, output)
    _peakExecutionMemory.write(kryo, output)
    _updatedBlockStatuses.write(kryo, output)
    inputMetrics.write(kryo, output)
    outputMetrics.write(kryo, output)
    shuffleReadMetrics.write(kryo, output)
    shuffleWriteMetrics.write(kryo, output)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    // read the TaskContext thread-local once
    val taskContext = TaskContext.get()
    _executorDeserializeTime.read(kryo, input, taskContext)
    _executorRunTime.read(kryo, input, taskContext)
    _resultSize.read(kryo, input, taskContext)
    _jvmGCTime.read(kryo, input, taskContext)
    _resultSerializationTime.read(kryo, input, taskContext)
    _memoryBytesSpilled.read(kryo, input, taskContext)
    _diskBytesSpilled.read(kryo, input, taskContext)
    _peakExecutionMemory.read(kryo, input, taskContext)
    _updatedBlockStatuses.read(kryo, input, taskContext)
    inputMetrics.read(kryo, input, taskContext)
    outputMetrics.read(kryo, input, taskContext)
    shuffleReadMetrics.read(kryo, input, taskContext)
    shuffleWriteMetrics.read(kryo, input, taskContext)
  }
}


private[spark] object TaskMetrics extends Logging {
  import InternalAccumulator._

  private val sparkTesting = sys.props.contains("spark.testing")

  private[spark] val internalAccumsNames = {
    val names = Array(
      EXECUTOR_DESERIALIZE_TIME,
      EXECUTOR_RUN_TIME,
      RESULT_SIZE,
      JVM_GC_TIME,
      RESULT_SERIALIZATION_TIME,
      MEMORY_BYTES_SPILLED,
      DISK_BYTES_SPILLED,
      PEAK_EXECUTION_MEMORY,
      UPDATED_BLOCK_STATUSES,
      shuffleRead.REMOTE_BLOCKS_FETCHED,
      shuffleRead.LOCAL_BLOCKS_FETCHED,
      shuffleRead.REMOTE_BYTES_READ,
      shuffleRead.LOCAL_BYTES_READ,
      shuffleRead.FETCH_WAIT_TIME,
      shuffleRead.RECORDS_READ,
      shuffleWrite.BYTES_WRITTEN,
      shuffleWrite.RECORDS_WRITTEN,
      shuffleWrite.WRITE_TIME,
      input.BYTES_READ,
      input.RECORDS_READ,
      output.BYTES_WRITTEN,
      output.RECORDS_WRITTEN
    )
    if (sparkTesting) names :+ TEST_ACCUM else names
  }
  private val internalAccumsMap = internalAccumsNames.zipWithIndex.toMap

  /**
   * Create an empty task metrics that doesn't register its accumulators.
   */
  def empty: TaskMetrics = {
    val tm = new TaskMetrics
    val names = TaskMetrics.internalAccumsNames
    val accums = tm._internalAccums
    for (i <- names.indices) {
      val name = names(i)
      val acc = accums(i)
      acc.metadata = AccumulatorMetadata(AccumulatorContext.newId(),
        Some(name), countFailedValues = true)
    }
    tm
  }

  def registered: TaskMetrics = {
    val tm = empty
    tm._internalAccums.foreach(AccumulatorContext.register)
    tm
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of [[AccumulableInfo]], called on driver only.
   * The returned [[TaskMetrics]] is only used to get some internal metrics, we don't need to take
   * care of external accumulator info passed in.
   */
  def fromAccumulatorInfos(infos: Seq[AccumulableInfo]): TaskMetrics = {
    val tm = new TaskMetrics
    val accums = tm._internalAccums
    infos.filter(info => info.name.isDefined && info.update.isDefined).foreach { info =>
      val name = info.name.get
      val value = info.update.get
      if (name == UPDATED_BLOCK_STATUSES) {
        tm.setUpdatedBlockStatuses(value.asInstanceOf[java.util.List[(BlockId, BlockStatus)]])
      } else {
        internalAccumsMap.get(name).foreach { index =>
          accums(index) match {
            case l: LongAccumulator => l.setValue(value.asInstanceOf[Long])
            case d: DoubleAccumulator => value match {
              case v: Long => d.setValue(v)
              case _ => d.setValue(value.asInstanceOf[Double])
            }
            case o => throw new UnsupportedOperationException(
              s"unexpected accumulator $o for TaskMetrics")
          }
        }
      }
    }
    tm
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of accumulator updates, called on driver only.
   */
  def fromAccumulators(accums: Seq[AccumulatorV2[_, _]]): TaskMetrics = {
    val tm = new TaskMetrics
    val metricsAccums = tm._internalAccums
    for (acc <- accums) {
      val internalId = acc.internalId
      if (internalId > 0) { // internal accumulator
        val metricsAcc = metricsAccums(internalId - 1).asInstanceOf[AccumulatorV2[Any, Any]]
        metricsAcc.metadata = acc.metadata
        metricsAcc.merge(acc.asInstanceOf[AccumulatorV2[Any, Any]])
      } else {
        tm.externalAccums += acc
      }
    }
    tm
  }
}


private[spark] class BlockStatusesAccumulator
  extends AccumulatorV2Kryo[(BlockId, BlockStatus), java.util.List[(BlockId, BlockStatus)]]
  with KryoSerializable {
  private val _seq = Collections.synchronizedList(new ArrayList[(BlockId, BlockStatus)]())

  override def isZero(): Boolean = _seq.isEmpty

  override def copyAndReset(): BlockStatusesAccumulator = new BlockStatusesAccumulator

  override def copy(): BlockStatusesAccumulator = {
    val newAcc = new BlockStatusesAccumulator
    newAcc._seq.addAll(_seq)
    newAcc
  }

  override def reset(): Unit = _seq.clear()

  override def add(v: (BlockId, BlockStatus)): Unit = _seq.add(v)

  override def merge(
    other: AccumulatorV2[(BlockId, BlockStatus), java.util.List[(BlockId, BlockStatus)]]): Unit = {
    other match {
      case o: BlockStatusesAccumulator => _seq.addAll(o.value)
      case _ => throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  override def value: java.util.List[(BlockId, BlockStatus)] = _seq

  def setValue(newValue: java.util.List[(BlockId, BlockStatus)]): Unit = {
    _seq.clear()
    _seq.addAll(newValue)
  }

  override def writeKryo(kryo: Kryo, output: Output): Unit = {
    val len = _seq.size()
    output.writeVarInt(len, true)
    var index = 0
    while (index < len) {
      val (id, status) = _seq.get(index)
      output.writeString(id.name)
      output.writeLong(status.memSize)
      output.writeLong(status.diskSize)
      val storageLevel = status.storageLevel
      output.writeByte(storageLevel.toInt)
      output.writeByte(storageLevel.replication)
      index += 1
    }
  }

  override def readKryo(kryo: Kryo, input: Input): Unit = {
    if (_seq.size() > 0) _seq.clear()
    var len = input.readVarInt(true)
    while (len > 0) {
      val id = BlockId(input.readString())
      val memSize = input.readLong()
      val diskSize = input.readLong()
      val levelFlags = input.readByte()
      val replication = input.readByte()
      val status = BlockStatus(StorageLevel(levelFlags, replication),
        memSize, diskSize)
      _seq.add(id -> status)

      len -= 1
    }
  }
}
