package com.susu.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage;
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
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;

/**
 * create by suhaha on 2019/9/19 13:43
 */
public class GenericUDAFMyAverage extends AbstractGenericUDAFResolver {

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
        if (parameters.length != 1) {//若入参个数不对，抛异常
            throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
        } else if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {//若入参数据类型不对，抛异常
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
        } else {
            switch(((PrimitiveTypeInfo)parameters[0]).getPrimitiveCategory()) {
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case STRING:
                case TIMESTAMP:
                    return new GenericUDAFMyAverage.GenericUDAFAverageEvaluator();
                case BOOLEAN:
                default:
                    throw new UDFArgumentTypeException(0, "Only numeric or string type arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
            }
        }

    }

    /**
     * 2、构造一个继承抽象类GenericUDAFEvaluator的类，并重写它的几个抽下你个方法
     */
    public static class GenericUDAFAverageEvaluator extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector inputOi;
        private DoubleWritable result;  //接受结果
        private Object[] partialResult; //存储terminatePartial方法返回值
        StructObjectInspector soi;
        StructField countField;
        StructField sumField;
        LongObjectInspector countFieldOI;
        DoubleObjectInspector sumFieldOI;
        boolean warned = false;

        /**
         * 确定各个阶段输入输出参数的数据格式 ObjectInspectors
         * 同时进行一些初始化操作
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert parameters.length == 0;
            //由super.init方法的具体实现可知，方法中将m赋值给了mode变量，因此在接下来的代码中，可以直接通过this.mode来引用m对象
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
                this.soi = (StructObjectInspector)parameters[0];
                this.countField = this.soi.getStructFieldRef("count");
                this.sumField = this.soi.getStructFieldRef("sum");
                this.countFieldOI = (LongObjectInspector)this.countField.getFieldObjectInspector();
                this.sumFieldOI = (DoubleObjectInspector)this.sumField.getFieldObjectInspector();
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
                this.result = new DoubleWritable(0.0D);//将result初始化为0
                /**
                 * 工厂方法PrimitiveObjectInspectorFactory中提供各个基本类型的Writable型ObjectInspector和Java型ObjectInspector的静态缓存
                 */
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            } else {
                /**
                 * `PARTIAL1`: `iterate` ──> `terminatePartial`
                 * `PARTIAL2` ： `merge` ──> `terminatePartial`
                 * 若当前是PARTIAL1或PARTIAL2阶段，则最后都是执行的terminatePartial方法，
                 * 因此这里要return的类型，就是terminatePartial方法的返回值类型
                 */
                //因为terminatePartial方法中要将运算结果保存到partialResult中，所以这里先将partialResult初始化一下
                this.partialResult = new Object[2];
                this.partialResult[0] = new LongWritable(0L);
                this.partialResult[1] = new DoubleWritable(0.0D);

                //最后这里才是跟terminatePartial方法的返回值类型的相关操作
                //1.1.Struct结构中各参数Field的 名称 指定，存放list中
                ArrayList<String> fname = new ArrayList();
                fname.add("count");
                fname.add("sum");
                //1.2.Struct结构中各参数Field的 类型 指定，存放list中
                ArrayList<ObjectInspector> foi = new ArrayList();
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                //1.3.其它类型的工厂方法获得Struct的OI实例（StandardStructObjectInspector）
                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
            }
        }

        /**
         * 获取buffer类
         */
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AvgAggBuffer avgAggBuffer = new AvgAggBuffer();
            this.reset(avgAggBuffer);
            return avgAggBuffer;
        }

        /**
         * 内部类，缓冲类
         */
        public class AvgAggBuffer implements AggregationBuffer {
            long count;
            double sum;
        }

        /**
         * 重置聚集结果buffer
         */
        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            AvgAggBuffer buffer = (AvgAggBuffer) aggregationBuffer;
            buffer.count = 0L;
            buffer.sum = 0.0D;
        }

        /**
         * map 阶段，迭代处理输入sql传过来的列数据
         */
        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters) throws HiveException {
            assert parameters.length == 1;
            Object parameter = parameters[0];
            if (parameter != null) {
                AvgAggBuffer buffer = (AvgAggBuffer) aggregationBuffer;
                try {
                    //通过基本类型工具类，依据 this.inputOi 设定的数据类型，读取参数parameter中数据
                    double value = PrimitiveObjectInspectorUtils.getDouble(parameter, this.inputOi);
                    ++buffer.count;
                    buffer.sum += value;
                } catch (NumberFormatException e) {
                    //只警告一次！
                    if (!this.warned) {
                        this.warned = true;
                        System.out.println(this.getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
                        System.out.println(this.getClass().getSimpleName() + " ignoring similar exceptions.");
                    }
                }
            }
        }

        /**
         * map 与 combiner 结束返回结果，得到部分数据聚集结果
         */
        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            AvgAggBuffer buffer = (AvgAggBuffer) aggregationBuffer;
            ((LongWritable)this.partialResult[0]).set(buffer.count);
            ((DoubleWritable)this.partialResult[1]).set(buffer.sum);
            return this.partialResult;
        }

        /**
         * combiner 合并 map 返回的结果，还有 reducer 合并 mapper 或 combiner 返回的结果。
         */
        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
            if (partial != null) {
                AvgAggBuffer buffer = (AvgAggBuffer) aggregationBuffer;
                //通过StructOI及指定的参数类型，将partial中的数据【按参数类型】分离出
                Object partialCount = this.soi.getStructFieldData(partial, this.countField);
                Object partialSum = this.soi.getStructFieldData(partial, this.sumField);
                //通过基本类型OI实例解析参数值
                buffer.count += this.countFieldOI.get(partialCount);
                buffer.sum += this.sumFieldOI.get(partialSum);
            }
        }

        /**
         * reducer 阶段，输出最终结果
         */
        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            AvgAggBuffer buffer = (AvgAggBuffer) aggregationBuffer;
            if (buffer.count == 0) {
                return null;
            } else {
                this.result.set(buffer.sum / (double)buffer.count);
                return result;
            }
        }


    }
}
