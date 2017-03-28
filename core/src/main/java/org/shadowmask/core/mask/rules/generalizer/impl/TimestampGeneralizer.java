/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.shadowmask.core.mask.rules.generalizer.impl;

import org.joda.time.DateTime;
import org.shadowmask.core.mask.rules.MaskRuntimeException;
import org.shadowmask.core.mask.rules.generalizer.Generalizer;

/**
 * TimestampGeneralizer support 8 mask levels, take "2016-10-21T10:22:28.123" for example:
 * - LEVEL0, 2016-10-21T10:22:28.123, mask nothing.
 * - LEVEL1, 2016-10-21T10:22:28.000, mask ms.
 * - LEVEL2, 2016-10-21T10:22:00.000, mask second.
 * - LEVEL2, 2016-10-21T10:00:00.000, mask minute.
 * - LEVEL2, 2016-10-21T00:00:00.000, mask hour.
 * - LEVEL2, 2016-10-01T00:00:00.000, mask day.
 * - LEVEL2, 2016-01-01T00:00:00.000, mask month.
 * - LEVEL2, 00-01-01T00:00:00.000, mask year.
 * <p>
 * NOTE: the timestamp is stored as long value, we use time string here for better understanding.
 */
public class TimestampGeneralizer implements Generalizer<Long, Long> {

  private static int ROOT_HIERARCHY_LEVEL = 7;

  @Override public Long generalize(Long timestamp, int hierarchyLevel) {
    if (timestamp == null) {
      return null;
    }

    if (hierarchyLevel > ROOT_HIERARCHY_LEVEL || hierarchyLevel < 0) {
      throw new MaskRuntimeException(
          "Root hierarchy level of MobileGeneralizer is " + ROOT_HIERARCHY_LEVEL
              +
              ", invalid input hierarchy level[" + hierarchyLevel + "]");
    }

    if (hierarchyLevel == 0) {
      return timestamp;
    }

    try {
      DateTime dateTime = new DateTime(timestamp);
      switch (hierarchyLevel) {
      // mask ms.
      case 1:
        dateTime = new DateTime(dateTime.getYear(), dateTime.getMonthOfYear(),
            dateTime.getDayOfMonth(), dateTime.getHourOfDay(),
            dateTime.getMinuteOfHour(), dateTime.getSecondOfMinute());
        break;
      // mask second.
      case 2:
        dateTime = new DateTime(dateTime.getYear(), dateTime.getMonthOfYear(),
            dateTime.getDayOfMonth(), dateTime.getHourOfDay(),
            dateTime.getMinuteOfHour());
        break;
      // mask minute.
      case 3:
        dateTime = new DateTime(dateTime.getYear(), dateTime.getMonthOfYear(),
            dateTime.getDayOfMonth(), dateTime.getHourOfDay(),
            dateTime.getMinuteOfHour());
        dateTime = dateTime.minuteOfHour().setCopy(0);
        break;
      // mask hour.
      case 4:
        dateTime = new DateTime(dateTime.getYear(), dateTime.getMonthOfYear(),
            dateTime.getDayOfMonth(), dateTime.getHourOfDay(),
            dateTime.getMinuteOfHour());
        dateTime = dateTime.minuteOfHour().setCopy(0);
        dateTime = dateTime.hourOfDay().setCopy(0);
        break;
      // mask day.
      case 5:
        dateTime = new DateTime(dateTime.getYear(), dateTime.getMonthOfYear(),
            dateTime.getDayOfMonth(), dateTime.getHourOfDay(),
            dateTime.getMinuteOfHour());
        dateTime = dateTime.minuteOfHour().setCopy(0);
        dateTime = dateTime.hourOfDay().setCopy(0);
        dateTime = dateTime.dayOfMonth().setCopy(1);
        break;
      // mask month.
      case 6:
        dateTime = new DateTime(dateTime.getYear(), dateTime.getMonthOfYear(),
            dateTime.getDayOfMonth(), dateTime.getHourOfDay(),
            dateTime.getMinuteOfHour());
        dateTime = dateTime.minuteOfHour().setCopy(0);
        dateTime = dateTime.hourOfDay().setCopy(0);
        dateTime = dateTime.dayOfMonth().setCopy(1);
        dateTime = dateTime.monthOfYear().setCopy(1);
        break;
      // mask year.
      case 7:
        dateTime = new DateTime(dateTime.getYear(), dateTime.getMonthOfYear(),
            dateTime.getDayOfMonth(), dateTime.getHourOfDay(),
            dateTime.getMinuteOfHour());
        dateTime = dateTime.minuteOfHour().setCopy(0);
        dateTime = dateTime.hourOfDay().setCopy(0);
        dateTime = dateTime.dayOfMonth().setCopy(1);
        dateTime = dateTime.monthOfYear().setCopy(1);
        dateTime = dateTime.year().setCopy(1901);
        break;
      }

      return dateTime.getMillis();
    } catch (Throwable e) {
      throw new MaskRuntimeException(
          "Invalid timestamp to generalize:" + timestamp, e);
    }
  }

  @Override public int getRootLevel() {
    return ROOT_HIERARCHY_LEVEL;
  }
}
