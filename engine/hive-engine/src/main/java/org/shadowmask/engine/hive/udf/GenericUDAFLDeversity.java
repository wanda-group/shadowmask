/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.shadowmask.engine.hive.udf;

import java.lang.StringBuilder;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ReflectionStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryStructObjectInspector;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * GenericUDAFLDeversity.
 */
@Description(name = "LDeversity",
        value = "_FUNC_(keyNum, key1, key2, ..., value1, value2, ...) : Calculate the L-Deversity\n"
                + "keyNum: the number of keys (key1, key2, ...)\n"
                + "key: the masked QUSI_IDENTIFIER columns\n"
                + "value: the sensitive columns\n"
                + "@return: a Statistic class containing minimal and mean L-Deversity value",
        extended = "Example:\n")
public class GenericUDAFLDeversity extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
          throws SemanticException {
    if(parameters.length == 0) {
      throw new UDFArgumentException("Arguments expected");
    }

    for(int i = 0; i < parameters.length; i++) {
      if(parameters[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentException("Only primitive type arguments are accepted");
      }
    }

    if (((PrimitiveTypeInfo)parameters[0]).getPrimitiveCategory()
        != PrimitiveObjectInspector.PrimitiveCategory.INT) {
      throw new UDFArgumentException("The first argument type should be INT");
    }

    return new GenericUDAFLDeversityEvaluator();
  }


  public static class GenericUDAFLDeversityEvaluator extends GenericUDAFEvaluator {
    private IntWritable result;

    private ObjectInspector inputKeyOI;
    private ObjectInspector inputValueOI;

    private StandardMapObjectInspector internalMergeOI;
    private StandardMapObjectInspector mergeOI;

    /** init not completed */
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      /*
       * In partial1, parameters are the inspectors of resultant columns
       * produced by a sql.
       */
      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        inputKeyOI = (JavaStringObjectInspector)
            ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

        inputValueOI = ObjectInspectorFactory.getReflectionObjectInspector(
            UDAFLDObject.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        return ObjectInspectorFactory.getStandardMapObjectInspector(
            ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI), inputValueOI);
      } else {
        if(m == Mode.COMPLETE) {
          return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        } else {
          if (parameters.length > 1) throw new UDFArgumentException("Init parameters are incorrect");
          if (!(parameters[0] instanceof StandardMapObjectInspector)) {
            throw new UDFArgumentException("Init error for merge: Parameter is not a standard map object inspector!");
          }

          internalMergeOI = (StandardMapObjectInspector) parameters[0];

          inputKeyOI = ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI.getMapKeyObjectInspector());
          inputValueOI = internalMergeOI.getMapValueObjectInspector();

          mergeOI = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);

          return ObjectInspectorFactory.getReflectionObjectInspector(Statistics.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        }
      }
    }

    /** class for storing frequency of different inputs. */
    @AggregationType
    static class FreqTable extends AbstractAggregationBuffer {
      HashMap<String, UDAFLDObject> freqMap;

      void put(Object[] values) {
        Integer key_columns_num = ((IntWritable) values[0]).get();
        StringBuilder key_str = new StringBuilder();
        StringBuilder value_str = new StringBuilder();
        for (int i = 1; i <= key_columns_num; i++) {
          key_str.append(values[i]);
        }

        for (int i = key_columns_num + 1; i < values.length; i++) {
          value_str.append(values[i]);
        }

        // generate an unique identifier for one row
        UDFUIdentifier uid = new UDFUIdentifier();
        Text txt = uid.evaluate(new Text(key_str.toString()));
        String key = txt.toString();

        txt = uid.evaluate(new Text(value_str.toString()));
        String value = txt.toString();

        UDAFLDObject sri = new UDAFLDObject(key, value);
        UDAFLDObject v = freqMap.get(sri.getRow());
        if(v == null) {
          sri.setCount(1);
          freqMap.put(sri.getRow(), sri);
        } else {
          v.put(sri.getSensitiveValue());
          v.increase(1);
        }
      }

      /**
       * return the last minimal frequent value in map
       */
      UDAFLDObject min() {
        int min = Integer.MAX_VALUE;
        if(freqMap.size() == 0) {
          return null;
        }
        UDAFLDObject result = null;
        for(UDAFLDObject value : freqMap.values()) {
          if(value.getDeversityNumber() < min) {
            min = value.getDeversityNumber();
            result = value;
          }
        }
        return result == null ? null : result;
      }

      /**
       * return the mean L-Deversity value in map
       */
      double mean() {
        double sum = 0;
        if(freqMap.isEmpty()) {
          return 0;
        }
        for(UDAFLDObject value : freqMap.values()) {
          sum += value.getDeversityNumber();
        }
        return sum/freqMap.size();
      }

      /**
       * return the statistics of L-Deversity value in map
       */
      Statistics calculateStatistics() {
        UDAFLDObject minValue = this.min();
        if(minValue == null) {
          return null;
        }
        Statistics result = new Statistics(minValue.getDeversityNumber(), this.mean());
        return result;
      }

    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      FreqTable buffer = new FreqTable();
      reset(buffer);
      return buffer;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((FreqTable) agg).freqMap = new HashMap<String, UDAFLDObject>();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      // TODO: Directly call merge(...) method.
      ((FreqTable) agg).put(parameters);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      FreqTable ft = (FreqTable) agg;
      HashMap<String, UDAFLDObject> ret = new HashMap<String, UDAFLDObject>();
      ret.putAll(ft.freqMap);
      ft.freqMap.clear();

      return ret;
    }

    /** NOTE: LazyBinaryMap's key object must be a wrtiable primitive objects*/
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      HashMap<Object, Object> result = (HashMap<Object, Object>) internalMergeOI.getMap(partial);
      FreqTable ft = (FreqTable) agg;

      for (Entry<Object, Object> e : result.entrySet()) {

        Text rowTxt = (Text)((LazyBinaryStruct)e.getValue()).getField(0);
        String row = rowTxt.toString();
        IntWritable count = (IntWritable)((LazyBinaryStruct)e.getValue()).getField(1);

        Text valueTxt = (Text)((LazyBinaryStruct)e.getValue()).getField(2);
        String value = valueTxt.toString();

        LazyBinaryMap lbm = (LazyBinaryMap)((LazyBinaryStruct)e.getValue()).getField(3);
        Map m = lbm.getMap();

        // merge all the patial maps
        UDAFLDObject v = ft.freqMap.get(row);
        if(v == null) {
          v = new UDAFLDObject(row);
          ft.freqMap.put(row, v);
        }
        for(Object entry : m.entrySet()) {
          String val = ((Entry)entry).getKey().toString();
          int valNumber = ((IntWritable)((Entry)entry).getValue()).get();
          v.put(val, valNumber);
        }
        v.increase(count.get());
      }
    }

    // The return class type to store the statistics of L-Deversity
    static class Statistics {
      public int minLDeversityValue = 0;
      public double meanLDeversityValue = 0;

      public Statistics () {
        minLDeversityValue = 0;
        meanLDeversityValue = 0;
      }

      public Statistics (int minLDeversityValue, double meanLDeversityValue) {
        this.minLDeversityValue = minLDeversityValue;
        this.meanLDeversityValue = meanLDeversityValue;
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return ((FreqTable)agg).calculateStatistics();
    }
  }

}
