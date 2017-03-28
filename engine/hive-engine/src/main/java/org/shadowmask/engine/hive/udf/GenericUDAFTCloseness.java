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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * GenericUDAFTCloseness.
 */
@Description(name = "TCloseness",
        value = "_FUNC_(key1, key2, ..., value) : Calculate the T-Closeness\n"
                + "key: the masked QUSI_IDENTIFIER columns\n"
                + "value: the sensitive columns (apply equal distance when the type is STRING or CHAR, " +
                "apply ordered distance when the type is LONG, INT, BYTE or SHORT, other types are not supported\n"
                + "@return: a TClosenessStatistics class containing minimal, maximal and mean T-Closeness value",
        extended = "Example:\n")
public class GenericUDAFTCloseness extends AbstractGenericUDAFResolver {
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

    switch (((PrimitiveTypeInfo)parameters[parameters.length - 1]).getPrimitiveCategory()) {
      case STRING:
      case CHAR:
        return new GenericUDAFTClosenessStringEvaluator();
      case LONG:
      case INT:
      case BYTE:
      case SHORT:
        return new GenericUDAFTClosenessLongEvaluator();
      case FLOAT:
      case DOUBLE:
        return new GenericUDAFTClosenessDoubleEvaluator();
      default:
        throw new UDFArgumentException("Only numerical and string type are accepted as sensitive attribute");
    }
  }


  public static class GenericUDAFTClosenessStringEvaluator extends GenericUDAFEvaluator {
    private ObjectInspector inputKeyOI;
    private ObjectInspector inputValueOI;

    private StandardMapObjectInspector internalMergeOI;

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
                UDAFTCStringObject.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
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

          return ObjectInspectorFactory.getReflectionObjectInspector(TClosenessStatistics.class,
                  ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        }
      }
    }
    

    /** class for storing frequency of different inputs. */
    @AggregationType
    static class FreqTable extends AbstractAggregationBuffer {
      HashMap<String, UDAFTCStringObject> freqMap;
      UDAFTCStringObject total;

      void put(Object[] values) {
        StringBuilder key_str = new StringBuilder();
        StringBuilder value_str = new StringBuilder();
        for (int i = 0; i < values.length - 1; i++) {
          key_str.append(values[i]);
        }

        value_str.append(values[values.length - 1]);

        // generate an unique identifier for one row
        /*UDFUIdentifier uid = new UDFUIdentifier();
        Text txt = uid.evaluate(new Text(key_str.toString()));
        String key = txt.toString();

        txt = uid.evaluate(new Text(value_str.toString()));
        String value = txt.toString();*/

        String key = key_str.toString();
        String value = value_str.toString();

        // add new item to freqMap
        UDAFTCStringObject sri = new UDAFTCStringObject(key, value);
        UDAFTCStringObject v = freqMap.get(sri.getRow());
        if(v == null) {
          sri.setCount(1);
          freqMap.put(sri.getRow(), sri);
        } else {
          v.put(sri.getSensitiveValue());
          v.increase(1);
        }

        // add new item to total
        total.put(sri.getSensitiveValue());
        total.increase(1);
      }


      /**
       * return the statistics of T-Closeness value in map
       */
      TClosenessStatistics calculateStatistics() {
        double min = Double.MAX_VALUE;
        double max = 0;
        double mean = 0;
        if (freqMap == null || freqMap.size() == 0) {
          return null;
        }

        ArrayList<Double> equalClass = new ArrayList<Double>();
        for (Map.Entry<String, UDAFTCStringObject> eachEqualClass : freqMap.entrySet()) {
          double sum = 0;
          Map<String, Integer> eachDeversity = eachEqualClass.getValue().getDeversities();
          for (Map.Entry<String, Integer> sensitiveAttribute : total.getDeversities().entrySet()) {
            String sensitiveAttributeValue = sensitiveAttribute.getKey();
            double q = (double)sensitiveAttribute.getValue()/total.getCount();
            double p = 0;
            Integer pNum = eachDeversity.get(sensitiveAttributeValue);
            if (pNum != null) {
              p = (double)pNum/eachEqualClass.getValue().getCount();
            }
            sum += Math.abs(p - q);
          }
          equalClass.add(sum/2);
        }
        // find max, min and mean
        for (Double e : equalClass) {
          min = e < min ? e : min;
          max = e > max ? e : max;
          mean += e;
        }
        mean /= equalClass.size();

        TClosenessStatistics result = new TClosenessStatistics(max, min, mean);

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
      ((FreqTable) agg).freqMap = new HashMap<String, UDAFTCStringObject>();
      ((FreqTable) agg).total = new UDAFTCStringObject("*");
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      // TODO: Directly call merge(...) method.
      ((FreqTable) agg).put(parameters);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      FreqTable ft = (FreqTable) agg;
      HashMap<String, UDAFTCStringObject> ret = new HashMap<String, UDAFTCStringObject>();
      ret.putAll(ft.freqMap);
      ft.freqMap.clear();

      return ret;
    }

    /** NOTE: LazyBinaryMap's key object must be a wrtiable primitive objects*/
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      HashMap<Object, Object> result = (HashMap<Object, Object>) internalMergeOI.getMap(partial);
      FreqTable ft = (FreqTable) agg;

      for (Map.Entry<Object, Object> e : result.entrySet()) {

        Text rowTxt = (Text)((LazyBinaryStruct)e.getValue()).getField(0);
        String row = rowTxt.toString();
        IntWritable count = (IntWritable)((LazyBinaryStruct)e.getValue()).getField(1);

        Text valueTxt = (Text)((LazyBinaryStruct)e.getValue()).getField(2);
        String value = valueTxt.toString();

        LazyBinaryMap lbm = (LazyBinaryMap)((LazyBinaryStruct)e.getValue()).getField(3);
        Map m = lbm.getMap();

        // merge all the patial maps
        UDAFTCStringObject v = ft.freqMap.get(row);
        if(v == null) {
          v = new UDAFTCStringObject(row);
          ft.freqMap.put(row, v);
        }
        for(Object entry : m.entrySet()) {
          String val = ((Map.Entry)entry).getKey().toString();
          int valNumber = ((IntWritable)((Map.Entry)entry).getValue()).get();
          v.put(val, valNumber);
          ft.total.put(val, valNumber);
        }
        v.increase(count.get());
        ft.total.increase(count.get());
      }

    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return ((FreqTable)agg).calculateStatistics();
    }
  }

  public static class GenericUDAFTClosenessDoubleEvaluator extends GenericUDAFEvaluator {
    private ObjectInspector inputKeyOI;
    private ObjectInspector inputValueOI;

    private StandardMapObjectInspector internalMergeOI;

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
        /*return ObjectInspectorFactory.getReflectionObjectInspector(FreqTable.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                */
        inputKeyOI = (JavaStringObjectInspector)
                ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        inputValueOI = ObjectInspectorFactory.getReflectionObjectInspector(
                UDAFTCDoubleObject.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
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

          return ObjectInspectorFactory.getReflectionObjectInspector(TClosenessStatistics.class,
                  ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        }
      }
    }


    /** class for storing frequency of different inputs. */
    @AggregationType
    static class FreqTable extends AbstractAggregationBuffer {
      HashMap<String, UDAFTCDoubleObject> freqMap;
      UDAFTCDoubleObject total;

      void put(Object[] values) {
        StringBuilder key_str = new StringBuilder();
        StringBuilder value_str = new StringBuilder();
        for (int i = 0; i < values.length - 1; i++) {
          key_str.append(values[i]);
        }
        value_str.append(values[values.length - 1]);

        // generate an unique identifier for one row
        /*UDFUIdentifier uid = new UDFUIdentifier();
        Text txt = uid.evaluate(new Text(key_str.toString()));
        String key = txt.toString();

        txt = uid.evaluate(new Text(value_str.toString()));
        String value = txt.toString();*/

        String key = key_str.toString();
        Double value = Double.parseDouble(value_str.toString());

        // add new item to freqMap
        UDAFTCDoubleObject sri = new UDAFTCDoubleObject(key, value);
        UDAFTCDoubleObject v = freqMap.get(sri.getRow());
        if(v == null) {
          sri.setCount(1);
          freqMap.put(sri.getRow(), sri);
        } else {
          v.put(sri.getSensitiveValue());
          v.increase(1);
        }

        // add new item to total
        total.put(sri.getSensitiveValue());
        total.increase(1);
      }


      /**
       * return the statistics of T-Closeness value in map
       */
      TClosenessStatistics calculateStatistics() {
        double min = Double.MAX_VALUE;
        double max = 0;
        double mean = 0;
        if (freqMap == null || freqMap.size() == 0) {
          return null;
        }

        ArrayList<Double> equalClass = new ArrayList<Double>();
        for (Map.Entry<String, UDAFTCDoubleObject> eachEqualClass : freqMap.entrySet()) {
          double sum = 0;
          double s = 0;
          Map<Double, Integer> eachDeversity = eachEqualClass.getValue().getDeversities();
          for (Map.Entry<Double, Integer> sensitiveAttribute : total.getDeversities().entrySet()) {
            Double sensitiveAttributeValue = sensitiveAttribute.getKey();
            double q = (double)sensitiveAttribute.getValue()/total.getCount();
            double p = 0;
            Integer pNum = eachDeversity.get(sensitiveAttributeValue);
            if (pNum != null) {
              p = (double)pNum/eachEqualClass.getValue().getCount();
            }
            s += p - q;
            sum += Math.abs(s);
          }
          equalClass.add(sum/(total.getCount()-1));
        }
        // find max, min and mean
        for (Double e : equalClass) {
          min = e < min ? e : min;
          max = e > max ? e : max;
          mean += e;
        }
        mean /= equalClass.size();

        TClosenessStatistics result = new TClosenessStatistics(max, min, mean);

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
      ((FreqTable) agg).freqMap = new HashMap<String, UDAFTCDoubleObject>();
      ((FreqTable) agg).total = new UDAFTCDoubleObject("*");
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      // TODO: Directly call merge(...) method.
      ((FreqTable) agg).put(parameters);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      FreqTable ft = (FreqTable) agg;
      HashMap<String, UDAFTCDoubleObject> ret = new HashMap<String, UDAFTCDoubleObject>();
      ret.putAll(ft.freqMap);
      ft.freqMap.clear();

      return ret;
    }

    /** NOTE: LazyBinaryMap's key object must be a wrtiable primitive objects*/
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      HashMap<Object, Object> result = (HashMap<Object, Object>) internalMergeOI.getMap(partial);
      FreqTable ft = (FreqTable) agg;

      for (Map.Entry<Object, Object> e : result.entrySet()) {

        Text rowTxt = (Text)((LazyBinaryStruct)e.getValue()).getField(0);
        String row = rowTxt.toString();
        IntWritable count = (IntWritable)((LazyBinaryStruct)e.getValue()).getField(1);

        LazyBinaryMap lbm = (LazyBinaryMap)((LazyBinaryStruct)e.getValue()).getField(3);
        Map m = lbm.getMap();

        // merge all the patial maps
        UDAFTCDoubleObject v = ft.freqMap.get(row);
        if(v == null) {
          v = new UDAFTCDoubleObject(row);
          ft.freqMap.put(row, v);
        }
        for(Object entry : m.entrySet()) {
          Double val = Double.parseDouble(((Map.Entry)entry).getKey().toString());
          int valNumber = ((IntWritable)((Map.Entry)entry).getValue()).get();
          v.put(val, valNumber);
          ft.total.put(val, valNumber);
        }
        v.increase(count.get());
        ft.total.increase(count.get());
      }

    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return ((FreqTable)agg).calculateStatistics();
    }
  }

  public static class GenericUDAFTClosenessLongEvaluator extends GenericUDAFEvaluator {
    private ObjectInspector inputKeyOI;
    private ObjectInspector inputValueOI;

    private StandardMapObjectInspector internalMergeOI;

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
        /*return ObjectInspectorFactory.getReflectionObjectInspector(FreqTable.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                */
        inputKeyOI = (JavaStringObjectInspector)
                ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        inputValueOI = ObjectInspectorFactory.getReflectionObjectInspector(
                UDAFTCLongObject.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
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

          return ObjectInspectorFactory.getReflectionObjectInspector(TClosenessStatistics.class,
                  ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        }
      }
    }


    /** class for storing frequency of different inputs. */
    @AggregationType
    static class FreqTable extends AbstractAggregationBuffer {
      HashMap<String, UDAFTCLongObject> freqMap;
      UDAFTCLongObject total;

      void put(Object[] values) {
        StringBuilder key_str = new StringBuilder();
        StringBuilder value_str = new StringBuilder();
        for (int i = 0; i < values.length - 1; i++) {
          key_str.append(values[i]);
        }
        value_str.append(values[values.length - 1]);

        // generate an unique identifier for one row
        /*UDFUIdentifier uid = new UDFUIdentifier();
        Text txt = uid.evaluate(new Text(key_str.toString()));
        String key = txt.toString();

        txt = uid.evaluate(new Text(value_str.toString()));
        String value = txt.toString();*/

        String key = key_str.toString();
        Long value = Long.parseLong(value_str.toString());

        // add new item to freqMap
        UDAFTCLongObject sri = new UDAFTCLongObject(key, value);
        UDAFTCLongObject v = freqMap.get(sri.getRow());
        if(v == null) {
          sri.setCount(1);
          freqMap.put(sri.getRow(), sri);
        } else {
          v.put(sri.getSensitiveValue());
          v.increase(1);
        }

        // add new item to total
        total.put(sri.getSensitiveValue());
        total.increase(1);
      }


      /**
       * return the statistics of T-Closeness value in map
       */
      TClosenessStatistics calculateStatistics() {
        double min = Double.MAX_VALUE;
        double max = 0;
        double mean = 0;
        if (freqMap == null || freqMap.size() == 0) {
          return null;
        }

        ArrayList<Double> equalClass = new ArrayList<Double>();
        for (Map.Entry<String, UDAFTCLongObject> eachEqualClass : freqMap.entrySet()) {
          double sum = 0;
          double s = 0;
          Map<Long, Integer> eachDeversity = eachEqualClass.getValue().getDeversities();
          for (Map.Entry<Long, Integer> sensitiveAttribute : total.getDeversities().entrySet()) {
            Long sensitiveAttributeValue = sensitiveAttribute.getKey();
            double q = (double)sensitiveAttribute.getValue()/total.getCount();
            double p = 0;
            Integer pNum = eachDeversity.get(sensitiveAttributeValue);
            if (pNum != null) {
              p = (double)pNum/eachEqualClass.getValue().getCount();
            }
            s += p - q;
            sum += Math.abs(s);
          }
          equalClass.add(sum/(total.getCount()-1));
        }
        // find max, min and mean
        for (Double e : equalClass) {
          min = e < min ? e : min;
          max = e > max ? e : max;
          mean += e;
        }
        mean /= equalClass.size();

        TClosenessStatistics result = new TClosenessStatistics(max, min, mean);

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
      ((FreqTable) agg).freqMap = new HashMap<String, UDAFTCLongObject>();
      ((FreqTable) agg).total = new UDAFTCLongObject("*");
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      // TODO: Directly call merge(...) method.
      ((FreqTable) agg).put(parameters);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      FreqTable ft = (FreqTable) agg;
      HashMap<String, UDAFTCLongObject> ret = new HashMap<String, UDAFTCLongObject>();
      ret.putAll(ft.freqMap);
      ft.freqMap.clear();

      return ret;
    }

    /** NOTE: LazyBinaryMap's key object must be a wrtiable primitive objects*/
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      HashMap<Object, Object> result = (HashMap<Object, Object>) internalMergeOI.getMap(partial);
      FreqTable ft = (FreqTable) agg;

      for (Map.Entry<Object, Object> e : result.entrySet()) {

        Text rowTxt = (Text)((LazyBinaryStruct)e.getValue()).getField(0);
        String row = rowTxt.toString();
        IntWritable count = (IntWritable)((LazyBinaryStruct)e.getValue()).getField(1);

        LazyBinaryMap lbm = (LazyBinaryMap)((LazyBinaryStruct)e.getValue()).getField(3);
        Map m = lbm.getMap();

        // merge all the patial maps
        UDAFTCLongObject v = ft.freqMap.get(row);
        if(v == null) {
          v = new UDAFTCLongObject(row);
          ft.freqMap.put(row, v);
        }
        for(Object entry : m.entrySet()) {
          Long val = Long.parseLong(((Map.Entry)entry).getKey().toString());
          int valNumber = ((IntWritable)((Map.Entry)entry).getValue()).get();
          v.put(val, valNumber);
          ft.total.put(val, valNumber);
        }
        v.increase(count.get());
        ft.total.increase(count.get());
      }

    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return ((FreqTable)agg).calculateStatistics();
    }
  }
}
