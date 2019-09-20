package com.susu.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

import java.util.ArrayList;

/**
 * create by suhaha on 2019/9/20 10:12
 */
public class GenericUDAFMyAverage2 extends AbstractGenericUDAFResolver {


    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {

        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "请输入一个且仅一个参数");
        } else if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "参数类型不对！只能输入基本类型！");
        } else {
            switch (((PrimitiveTypeInfo)parameters[0]).getPrimitiveCategory()) {
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case STRING:
                case TIMESTAMP:
                    return new GenericUDAFMyAverage2.GenericUDAFAverageEvaluator();
                case BOOLEAN:
                default:
                    throw new UDFArgumentTypeException(0, "Only numeric or string type arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
            }
        }
    }

    public static class GenericUDAFAverageEvaluator extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector inputOi;
        private DoubleWritable result;
        private StructObjectInspector soi;
        private StructField countField;
        private StructField sumField;
        private LongObjectInspector countFieldOi;
        private DoubleObjectInspector sumFieldOi;
        Object[] partialResult;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert parameters.length == 1;
            super.init(m, parameters);

            /**
             * 1、设置入参
             */
            if (m != Mode.PARTIAL1 && m != Mode.COMPLETE) {
                /**
                 * `PARTIAL2` ： `merge` ──> `terminatePartial`
                 * `FINAL`    ： `merge` ──> `terminate`
                 *  无论当前是PARTIAL2还是FINAL阶段，第一个执行的都是merge方法
                 */

                this.soi = (StructObjectInspector) parameters[0];
                this.countField = soi.getStructFieldRef("count");
                this.sumField = soi.getStructFieldRef("sum");
                this.countFieldOi = (LongObjectInspector)this.countField.getFieldObjectInspector();
                this.sumFieldOi = (DoubleObjectInspector)this.sumField.getFieldObjectInspector();
            } else {
                /**
                 * `PARTIAL1`: `iterate` ──> `terminatePartial`
                 * `COMPLETE`: `iterate` ──> `terminate`
                 * 无论当前阶段是PARTIAL1还是COMPLETE阶段，第一个执行的都是iterate方法，入参都是每一条原始的输入数据，
                 * 因此这里将入参parameters[0]转变为PrimitiveObjectInspector（基本类型）并赋值给inputOi，
                 */
                this.inputOi = (PrimitiveObjectInspector) parameters[0];
            }

            /**
             * 2、设置出参
             */
            if (m != Mode.PARTIAL1 && m != Mode.PARTIAL2) {
                /**
                 * `COMPLETE`: `iterate` ──> `terminate`
                 * `FINAL`    ： `merge` ──> `terminate`
                 * 若当前是COMPLETE或FINAL阶段，则最后都是调用terminate方法，
                 * 因此这里要return的类型，其实就terminate方法的返回值类型
                 * 调用terminate方法得到的返回值，就是当前所自定义的UDAF的返回值
                 * 因此这里的类型应该根据自定义的UDAF的实际功能来决定，比如，如果自定义UDAF的功能
                 * 是类似于concat那样把一个group的name连接起来，那么显然这里返回的类型应该是WritableStringObjectInspector
                 * 那么，根据当前自定义UDAF的功能（求平均数），可知最后应该返回的结果值应该是writableDoubleObjectInspector类型
                 */
                result = new DoubleWritable(0.0D);
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            } else {
                /**
                 * `PARTIAL1`: `iterate` ──> `terminatePartial`
                 * `PARTIAL2` ： `merge` ──> `terminatePartial`
                 * 若当前是PARTIAL1或PARTIAL2阶段，则最后都是执行的terminatePartial方法，
                 * 因此这里要return的类型，就是terminatePartial方法的返回值类型
                 */
                partialResult = new Object[2];
                partialResult[0] = new LongWritable();
                partialResult[1] = new DoubleWritable();

                ArrayList<String> fname = new ArrayList<>();
                fname.add("count");
                fname.add("sum");
                ArrayList<ObjectInspector> foi = new ArrayList<>();
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            GenericUDAFAverageEvaluator.AggBuffer buffer = new GenericUDAFAverageEvaluator.AggBuffer();
            this.reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            AggBuffer buffer = (AggBuffer) aggregationBuffer;
            buffer.count = 0L;
            buffer.sum = 0.0D;
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters) throws HiveException {
            assert parameters.length == 1;

            AggBuffer buffer = (AggBuffer) aggregationBuffer;
            Object parameter = parameters[0];

            if (parameter != null) {
                double value = PrimitiveObjectInspectorUtils.getDouble(parameter, this.inputOi);
                ++buffer.count;
                buffer.sum += value;
            }

        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            AggBuffer buffer = (AggBuffer) aggregationBuffer;
            ((LongWritable)this.partialResult[0]).set(buffer.count);
            ((DoubleWritable)this.partialResult[1]).set(buffer.sum);
            return partialResult;
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
            AggBuffer buffer = (AggBuffer) aggregationBuffer;

            Object countData = this.soi.getStructFieldData(partial, countField);
            Object sumData = this.soi.getStructFieldData(partial, sumField);

            long count = this.countFieldOi.get(countData);
            double sum = this.sumFieldOi.get(sumData);

            buffer.count += count;
            buffer.sum += sum;
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            AggBuffer buffer = (AggBuffer) aggregationBuffer;
            if (buffer.count == 0) {
                return null;
            } else {
                this.result.set(buffer.sum / (double)buffer.count);
                return result;
            }
        }

        public class AggBuffer implements AggregationBuffer{
            long count;
            double sum;
        }
    }
}
