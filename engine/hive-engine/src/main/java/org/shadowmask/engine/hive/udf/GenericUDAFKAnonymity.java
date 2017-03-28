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

import java.util.HashMap;
import java.util.Map;
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
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * GenericUDAFKAnonymity.
 */
@Description(name = "KAnonymity",
        value = "_FUNC_( key1, key2, ...) : Calculate the K-Anonymity"
                + "key: the masked QUSI_IDENTIFIER columns\n"
                + "@return: a Statistic class containing minimal and mean K-Anonymity value",
        extended = "Example:\n")
public class GenericUDAFKAnonymity extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
          throws SemanticException {
    if(parameters.length == 0) {
      throw new UDFArgumentException("Argument expected");
    }
    for(int i = 0; i < parameters.length; i++) {
      if(parameters[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentException("Only primitive type arguments are accepted");
      }
    }
    return new GenericUDAFKAnonymityEvaluator();
  }

  public static class GenericUDAFKAnonymityEvaluator extends GenericUDAFEvaluator {
    private IntWritable result;

    private ObjectInspector inputKeyOI;
    private ObjectInspector inputValueOI;

    private StandardMapObjectInspector internalMergeOI;
    private StandardMapObjectInspector mergeOI;

    /** init not completed */
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      /*
       * In partial1, parameters are the inspectors of resultant columns produced by a sql.
       */

      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        inputKeyOI = (JavaStringObjectInspector)
            ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

        inputValueOI = ObjectInspectorFactory.getReflectionObjectInspector(
            UDAFObject.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        return ObjectInspectorFactory.getStandardMapObjectInspector(
            ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI), inputValueOI);
      } else {
        if(m == Mode.COMPLETE) {
          return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        } else {
          if (parameters.length > 1) throw new UDFArgumentException("init parameters are incorrect");
          if (!(parameters[0] instanceof StandardMapObjectInspector)) {
            throw new UDFArgumentException("Init error for merge: parameter is not a standard map object inspector!");
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
      HashMap<String, UDAFObject> freqMap;

      void put(Object[] values) {
        String str = "";
        for (Object o : values) {
          str += o;
        }

        // generate an unique identifier for one row
        UDFUIdentifier uid = new UDFUIdentifier();
        Text txt = uid.evaluate(new Text(str));
        String code = txt.toString();

        UDAFObject sri = new UDAFObject(code);
        UDAFObject v = freqMap.get(sri.getRow());
        if(v == null) {
          sri.setCount(1);
          freqMap.put(sri.getRow(), sri);
        } else {
          sri.increase(v.getCount());
          freqMap.put(sri.getRow(), sri);
        }
      }

      /**
       * return the last minimal frequent value in map
       */
      UDAFObject min() {
        int min = Integer.MAX_VALUE;
        if(freqMap.size() == 0) {
          return null;
        }
        UDAFObject result = null;
        for(UDAFObject value : freqMap.values()) {
          if(value.getCount() < min) {
            min = value.getCount();
            result = value;
          }
        }
        return result == null ? null : result;
      }

      /**
       * return the mean K-Anonymity value in map
       */
      double mean() {
        double sum = 0;
        if(freqMap.isEmpty()) {
          return 0;
        }
        for(UDAFObject value : freqMap.values()) {
          sum += value.getCount();
        }
        return sum/freqMap.size();
      }

      /**
       * return the statistics of K-Anonymity value in map
       */
      Statistics calculateStatistics() {
        UDAFObject minValue = this.min();
        if(minValue == null) {
          return null;
        }
        Statistics result = new Statistics(minValue.getCount(), this.mean());
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
      ((FreqTable) agg).freqMap = new HashMap<String, UDAFObject>();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      // TODO: Directly call merge(...) to enhance the performance.
      ((FreqTable) agg).put(parameters);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      FreqTable ft = (FreqTable) agg;
      HashMap<String, UDAFObject> ret = new HashMap<String, UDAFObject>(ft.freqMap);

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

        UDAFObject sri = new UDAFObject(row);
        sri.setCount(count.get());

        // merge all the patial maps
        if (ft.freqMap.containsKey(sri.getRow())) {
            UDAFObject base = ft.freqMap.get(sri.getRow());
            sri.increase(base.getCount());
            ft.freqMap.put(sri.getRow(), sri);
        } else {
            ft.freqMap.put(sri.getRow(), sri);
        }
      }
    }

    // The return class type to store the statistics of K-Anonymity
    static class Statistics {
      public int minKAnonymityValue = 0;
      public double meanKAnonymityValue = 0;

      public Statistics () {
        minKAnonymityValue = 0;
        meanKAnonymityValue = 0;
      }

      public Statistics (int minKAnonymityValue, double meanKAnonymityValue) {
        this.minKAnonymityValue = minKAnonymityValue;
        this.meanKAnonymityValue = meanKAnonymityValue;
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return ((FreqTable)agg).calculateStatistics();
    }

  }

}
