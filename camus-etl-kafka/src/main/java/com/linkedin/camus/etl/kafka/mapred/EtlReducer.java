/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlReduceKey;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Reducer to compute basic metrics on a per topic:partition basis
 */
public class EtlReducer extends Reducer<EtlReduceKey, Object, EtlKey, Object> {

  @Override
  protected void reduce(EtlReduceKey key, Iterable<Object> values, Context context)
      throws IOException, InterruptedException {

    long skippedRecords = 0;
    long totalRecords = 0;

    EtlKey etlKey = new EtlKey(key);

    for(Object value : values) {
      if(SkippedRecordException.class.isInstance(value)) {
        skippedRecords++;
        totalRecords++;
        if (((SkippedRecordException)value).fullPrint) {
          context.write(etlKey, value);
        }
      } else if(CamusWrapper.class.isInstance(value)) {
        totalRecords++;
        context.write(etlKey, value);
      } else {
        context.write(etlKey, value);
      }
    }

    if (skippedRecords > 0) {
      double percentageSkipped = (double)skippedRecords / totalRecords;
      String skippedRecordsMessage = String.format(
          "Skipped records percentage for topic %s, partition %d: %f",
          key.getTopic(),
          key.getPartition(),
          percentageSkipped);
      context.write(etlKey, new ExceptionWritable(skippedRecordsMessage));
    }

  }
}
