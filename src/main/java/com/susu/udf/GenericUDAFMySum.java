package com.susu.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * create by suhaha on 2019/9/18 19:43
 */

public class GenericUDAFMySum extends AbstractGenericUDAFResolver {

    /**
     * 1、重写getEvaluator方法
     * 该方法的主要目的是校验UDAF的入参个数和入参类型并返回Evaluator对象
     * 调用者传入不同的参数时，向其返回不同的Evaluator或者直接抛出异常。
     * @param parameters
     * @return
     * @throws SemanticException
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {

        //1）先判断入参的个数
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "必须传入一个且仅一个入参！");
            //2）接着判断入参的数据类型
        } else if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "参数必须是基本类型，但是却传入了" + parameters[0].getTypeName() + "类型！");
            //3）然后根据入参的类型，用switch判断应该返回的Evaluator类型
        } else {
            switch (((PrimitiveTypeInfo)parameters[0]).getPrimitiveCategory()) {
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                    return new GenericUDAFSumElevator();
                default:
                    throw new UDFArgumentTypeException(0, "只能传输数值类型的入参，但是却传入了 " + parameters[0].getTypeName() + " 类型！");
            }
        }

    }


    /**
     * 2、构造一个继承抽象类GenericUDAFEvaluator的类，并重写它的几个抽下你个方法
     */
    public static class GenericUDAFSumElevator extends GenericUDAFEvaluator{

        private PrimitiveObjectInspector inputOi;
        private LongWritable result;
        private boolean warned = false;

        /**
         * 确定各个阶段输入输出参数的数据格式 ObjectInspectors
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert parameters.length == 1;
            super.init(m, parameters);

            this.result = new LongWritable(0L);
            //获取入参的数据类型
            this.inputOi = (PrimitiveObjectInspector) parameters[0];

            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        /**
         * 保存数据聚集结果的类
         */
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumAgg result = new SumAgg();
            this.reset(result);
            return result;
        }

        /**
         * 定义缓冲类，可以根据需求来自定义
         */
        static class SumAgg implements AggregationBuffer {
            boolean empty;
            long sum;
        }

        /**
         * 重置聚集结果
         */
        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            SumAgg sumAggBuffer = (SumAgg) aggregationBuffer;
            sumAggBuffer.empty = true;
            sumAggBuffer.sum = 0L;
        }

        /**
         * map 阶段，迭代处理输入sql传过来的列数据
         */
        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters) throws HiveException {
            assert parameters.length == 1;
            try {
                this.merge(aggregationBuffer, parameters[0]);
            } catch (NumberFormatException e) {
                if (!this.warned) {
                    this.warned = true;
                    System.out.println(this.getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
                }
            }
        }

        /**
         * map 与 combiner 结束返回结果，得到部分数据聚集结果
         */
        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return this.terminate(aggregationBuffer);
        }

        /**
         * combiner 合并 map 返回的结果，还有 reducer 合并 mapper 或combiner 返回的结果。
         */
        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
            if (partial != null) {
                SumAgg sumAggBuffer = (SumAgg) aggregationBuffer;
                sumAggBuffer.empty = false;
                //根据两个参数partial和this.inputOi，getDouble方法将this.inutOi类型的partial转换为了double类型
                sumAggBuffer.sum += PrimitiveObjectInspectorUtils.getDouble(partial, this.inputOi);
            }
        }

        /**
         * reducer 阶段，输出最终结果
         */
        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            SumAgg sumAggBuffer = (SumAgg) aggregationBuffer;

            if (sumAggBuffer.empty) { //若缓冲区为空，则返回空
                return null;
            } else {    //若缓冲区不为空，则取出缓冲区的数据，并添加到result变量中
                this.result.set(sumAggBuffer.sum);
                return this.result;
            }
        }
    }

}
