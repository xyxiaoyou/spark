/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.streaming.ui

import java.util.UUID

import scala.collection.mutable

import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}

class SnappyStreamingQueryListener extends StreamingQueryListener {

  private[streaming] val queries = new mutable.HashMap[UUID, StreamingQueryProgress]

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {

  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    queries.put(event.progress.id, event.progress)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    queries.remove(event.id)
  }
}
