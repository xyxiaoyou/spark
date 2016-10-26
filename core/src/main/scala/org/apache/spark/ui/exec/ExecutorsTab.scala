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

package org.apache.spark.ui.exec

import scala.collection.mutable

import org.apache.spark.{ExceptionFailure, Resubmitted, SparkConf, SparkContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.storage.{StorageStatus, StorageStatusListener}
import org.apache.spark.ui.{SparkUI, SparkUITab}
import org.apache.spark.ui.jobs.UIData.ExecutorUIData

private[ui] class ExecutorsTab(parent: SparkUI) extends SparkUITab(parent, "executors") {
  val listener = parent.executorsListener
  val sc = parent.sc
  val threadDumpEnabled =
    sc.isDefined && parent.conf.getBoolean("spark.ui.threadDumpsEnabled", true)

  attachPage(new ExecutorsPage(this, threadDumpEnabled))
  if (threadDumpEnabled) {
    attachPage(new ExecutorThreadDumpPage(this))
  }
}

/**
 * :: DeveloperApi ::
 * A SparkListener that prepares information to be displayed on the ExecutorsTab
 */
@DeveloperApi
class ExecutorsListener(storageStatusListener: StorageStatusListener, conf: SparkConf)
    extends SparkListener {

  private[spark] val executorIdToData = mutable.HashMap[String, ExecutorData]()

  def activeStorageStatusList: Seq[StorageStatus] = storageStatusListener.storageStatusList

  def deadStorageStatusList: Seq[StorageStatus] = storageStatusListener.deadStorageStatusList

  def getExecutorData(eid: String): ExecutorData = {
    executorIdToData.getOrElseUpdate(eid, new ExecutorData)
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = synchronized {
    val eid = executorAdded.executorId
    val data = getExecutorData(eid)
    data.logUrls = executorAdded.executorInfo.logUrlMap
    data.totalCores = executorAdded.executorInfo.totalCores
    data.tasksMax = data.totalCores / conf.getInt("spark.task.cpus", 1)
    data.uiData = new ExecutorUIData(executorAdded.time)
  }

  override def onExecutorRemoved(
      executorRemoved: SparkListenerExecutorRemoved): Unit = synchronized {
    val eid = executorRemoved.executorId
    val uiData = getExecutorData(eid).uiData
    uiData.finishTime = Some(executorRemoved.time)
    uiData.finishReason = Some(executorRemoved.reason)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    applicationStart.driverLogs.foreach { logs =>
      val storageStatus = activeStorageStatusList.find { s =>
        s.blockManagerId.executorId == SparkContext.LEGACY_DRIVER_IDENTIFIER ||
        s.blockManagerId.executorId == SparkContext.DRIVER_IDENTIFIER
      }
      storageStatus.foreach { s =>
        getExecutorData(s.blockManagerId.executorId).logUrls = logs.toMap }
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    val eid = taskStart.taskInfo.executorId
    getExecutorData(eid).tasksActive += 1
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    if (info != null) {
      val eid = info.executorId
      val data = getExecutorData(eid)
      taskEnd.reason match {
        case Resubmitted =>
          // Note: For resubmitted tasks, we continue to use the metrics that belong to the
          // first attempt of this task. This may not be 100% accurate because the first attempt
          // could have failed half-way through. The correct fix would be to keep track of the
          // metrics added by each attempt, but this is much more complicated.
          return
        case e: ExceptionFailure => data.tasksFailed += 1
        case _ => data.tasksComplete += 1
      }

      data.tasksActive -= 1
      data.duration += info.duration

      // Update shuffle read/write
      val metrics = taskEnd.taskMetrics
      if (metrics != null) {
        data.inputBytes += metrics.inputMetrics.bytesRead
        data.inputRecords += metrics.inputMetrics.recordsRead
        data.outputBytes += metrics.outputMetrics.bytesWritten
        data.outputRecords += metrics.outputMetrics.recordsWritten

        data.shuffleRead += metrics.shuffleReadMetrics.remoteBytesRead
        data.shuffleWrite += metrics.shuffleWriteMetrics.bytesWritten
        data.jvmGCTime += metrics.jvmGCTime
      }
    }
  }

}

private[spark] final class ExecutorData {
  var totalCores: Int = _
  var tasksMax: Int = _
  var tasksActive: Int = _
  var tasksComplete: Int = _
  var tasksFailed: Int = _
  var duration: Long = _
  var jvmGCTime: Long = _
  var inputBytes: Long = _
  var inputRecords: Long = _
  var outputBytes: Long = _
  var outputRecords: Long = _
  var shuffleRead: Long = _
  var shuffleWrite: Long = _
  var logUrls: Map[String, String] = Map.empty
  var uiData: ExecutorUIData = _
}
