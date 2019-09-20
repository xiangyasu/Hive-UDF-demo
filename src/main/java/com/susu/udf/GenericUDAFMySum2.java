package com.susu.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * create by suhaha on 2019/9/20 14:48
 */
public class GenericUDAFMySum2 extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "请输入一个且仅一个参数");
        } else if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException(0, "函数仅接收基础类型参数");
        } else {
            switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                    return new GenericUDAFMySum2.GenericUDAFSumElevator();
                default:
                    throw new UDFArgumentTypeException(0, "只能传输数值类型的入参，但是却传入了 " + parameters[0].getTypeName() + " 类型！");
            }
        }
    }

    public static class GenericUDAFSumElevator extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector inputOi;
        private LongWritable result;
        private boolean warned = false;
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert parameters.length == 1;
            super.init(m, parameters);
            this.inputOi = (PrimitiveObjectInspector) parameters[0];
            this.result = new LongWritable(0L);
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumAggBuffer buffer = new GenericUDAFSumElevator.SumAggBuffer();
            this.reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            SumAggBuffer buffer = (SumAggBuffer) aggregationBuffer;
            buffer.sum = 0;
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters) throws HiveException {
            assert parameters.length == 1;
            SumAggBuffer buffer = (SumAggBuffer) aggregationBuffer;
            Object parameter = parameters[0];
            try {
                double value = PrimitiveObjectInspectorUtils.getDouble(parameter, this.inputOi);
                buffer.sum += value;
            } catch (NumberFormatException e) {
                if (!this.warned) {
                    this.warned = true;
                    System.out.println(this.getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
                }
            }

        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            SumAggBuffer buffer = (SumAggBuffer) aggregationBuffer;
            this.result.set(buffer.sum);
            return result;
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
            SumAggBuffer buffer = (SumAggBuffer) aggregationBuffer;
            if (partial != null) {
                long value = PrimitiveObjectInspectorUtils.getLong(partial, this.inputOi);
                buffer.sum += value;
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            SumAggBuffer buffer = (SumAggBuffer) aggregationBuffer;
            this.result.set(buffer.sum);
            return this.result;
        }

        public class SumAggBuffer implements AggregationBuffer{
            long sum;
        }
    }
}
