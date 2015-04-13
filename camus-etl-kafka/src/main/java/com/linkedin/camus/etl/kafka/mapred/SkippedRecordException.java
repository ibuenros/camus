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

import com.linkedin.camus.etl.kafka.common.ExceptionWritable;


/**
 * Represents a skipped record
 */
public class SkippedRecordException extends ExceptionWritable{

  public final boolean fullPrint;

  public SkippedRecordException(Exception exc, boolean fullPrint) {
    super("Skipped record.", exc);
    this.fullPrint = fullPrint;
  }

}
