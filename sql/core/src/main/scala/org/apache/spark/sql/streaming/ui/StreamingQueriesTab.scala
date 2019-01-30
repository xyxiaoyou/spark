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

import org.apache.spark.internal.Logging
import org.apache.spark.ui.{SparkUI, SparkUITab}
import org.apache.spark.{SparkContext, SparkException}

class StreamingQueriesTab(sc: SparkContext)
    extends SparkUITab(StreamingQueriesTab.getSparkUI(sc), "streamingQueries") with Logging {

  import StreamingQueriesTab._
  def attach() {
    getSparkUI(sc).attachTab(this)
  }

  attachPage(new StreamingQueriesPage)
}

object StreamingQueriesTab{
  def getSparkUI(sc: SparkContext): SparkUI = {
    sc.ui.getOrElse{
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }
}
