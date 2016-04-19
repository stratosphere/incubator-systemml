/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysml.runtime.instructions.flink.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.lops.PartialAggregate;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.functionobjects.KahanPlus;
import org.apache.sysml.runtime.instructions.flink.functions.AggregateSingleBlockFunction;
import org.apache.sysml.runtime.instructions.flink.functions.ComputeBinaryBlockNnzFunction;
import org.apache.sysml.runtime.instructions.spark.data.CorrMatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;

public class DataSetAggregateUtils {

	public static DataSet<Tuple2<MatrixIndexes, MatrixBlock>> mergeByKey(
            DataSet<Tuple2<MatrixIndexes, MatrixBlock>> input) {
        return input.groupBy(0).reduce(new MergeBlocksFunction());
    }

    public static MatrixBlock sumStable(DataSet<MatrixBlock> input) throws DMLRuntimeException {

        try {
            return input.reduce(new SumSingleBlockFunction()).collect().get(0);
        } catch (Exception e) {
            throw new DMLRuntimeException("Could not collect final block of " + input);
        }
    }

	/**
	 *
	 * @param in
	 * @return
	 */
	public static MatrixBlock sumStable1( DataSet<Tuple2<MatrixIndexes, MatrixBlock>> in ) {
		try {
			return (MatrixBlock) in.map(new DataSetConverterUtils.ExtractElement(1)).returns(MatrixBlock.class).reduce(
					new SumSingleBlockFunction() ).collect().get(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

	/**
	 *
	 */
	private static class ExtractMatrixBlock implements MapFunction<Tuple2<MatrixIndexes, CorrMatrixBlock>, MatrixBlock >
	{
		@Override
		public MatrixBlock map(Tuple2<MatrixIndexes, CorrMatrixBlock> arg0)
			throws Exception
		{
			return arg0.f1.getValue();
		}
	}

	/**
	 *
	 * @param in
	 * @return
	 */
	public static DataSet<Tuple2<MatrixIndexes, MatrixBlock>> sumByKeyStable( DataSet<Tuple2<MatrixIndexes, MatrixBlock>> in )
	{
		//stable sum of blocks per key, by passing correction blocks along with aggregates 		
		DataSet<Tuple2<MatrixIndexes, CorrMatrixBlock>> tmp =
			in.groupBy(0). new CreateBlockCombinerFunction(),
				new MergeSumBlockValueFunction(),
				new MergeSumBlockCombinerFunction() );

		//strip-off correction blocks from 					     
		DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out =
			tmp.map( new ExtractMatrixBlock() );

		//return the aggregate rdd
		return out;
	}

    public static MatrixBlock aggStable(DataSet<Tuple2<MatrixIndexes, MatrixBlock>> in,
                                        AggregateOperator aop) throws DMLRuntimeException {
        //stable aggregate of all blocks with correction block per function instance
        DataSet<MatrixBlock> out = in.map(new MapFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixBlock>() {
            @Override
            public MatrixBlock map(Tuple2<MatrixIndexes, MatrixBlock> value) throws Exception {
                return value.f1;
            }
        }).reduce(new AggregateSingleBlockFunction(aop));

        try {
            return out.collect().get(0);
        } catch (Exception e) {
            throw new DMLRuntimeException("Could not collect the reduced MatrixBlock!", e);
        }
    }

    public static DataSet<Tuple2<MatrixIndexes, MatrixBlock>> aggByKeyStable(
            DataSet<Tuple2<MatrixIndexes, MatrixBlock>> in, final AggregateOperator aop) {
        //stable sum of blocks per key, by passing correction blocks along with aggregates

        DataSet<Tuple2<MatrixIndexes, CorrMatrixBlock>> output = in.map(
                new MapFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, CorrMatrixBlock>>() {
                    @Override
                    public Tuple2<MatrixIndexes, CorrMatrixBlock> map(
                            Tuple2<MatrixIndexes, MatrixBlock> value) throws Exception {
                        return new Tuple2<MatrixIndexes, CorrMatrixBlock>(value.f0, new CorrMatrixBlock(value.f1));
                    }
                }).groupBy(0).reduce(new ReduceFunction<Tuple2<MatrixIndexes, CorrMatrixBlock>>() {
            @Override
            public Tuple2<MatrixIndexes, CorrMatrixBlock> reduce(Tuple2<MatrixIndexes, CorrMatrixBlock> arg0,
                                                                 Tuple2<MatrixIndexes, CorrMatrixBlock> arg1) throws Exception {
                //get current block and correction
                MatrixBlock value1 = arg0.f1.getValue();
                MatrixBlock value2 = arg1.f1.getValue();
                MatrixBlock corr = arg0.f1.getCorrection();

                //correction block allocation on demand (but use second if exists)
                if (corr == null && aop.correctionExists) {
                    corr = (arg1.f1.getCorrection() != null) ? arg1.f1.getCorrection() :
                            new MatrixBlock(value1.getNumRows(), value1.getNumColumns(), false);
                }

                //aggregate other input and maintain corrections
                //(existing value and corr are used in place)
                if (aop.correctionExists)
                    OperationsOnMatrixValues.incrementalAggregation(value1, corr, value2, aop, true);
                else
                    OperationsOnMatrixValues.incrementalAggregation(value1, null, value2, aop, true);
                return new Tuple2<MatrixIndexes, CorrMatrixBlock>(arg0.f0, new CorrMatrixBlock(value1, corr));
            }
        });

        return output.map(
                new MapFunction<Tuple2<MatrixIndexes, CorrMatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>>() {
                    @Override
                    public Tuple2<MatrixIndexes, MatrixBlock> map(
                            Tuple2<MatrixIndexes, CorrMatrixBlock> value) throws Exception {
                        return new Tuple2<MatrixIndexes, MatrixBlock>(value.f0, value.f1.getValue());
                    }
                });
    }

    private static class SumSingleBlockFunction implements ReduceFunction<MatrixBlock> {

        private AggregateOperator _op = null;
        private MatrixBlock _corr = null;

        public SumSingleBlockFunction() {
            _op = new AggregateOperator(0, KahanPlus.getKahanPlusFnObject(), true,
                    PartialAggregate.CorrectionLocationType.NONE);
            _corr = null;
        }

        @Override
        public MatrixBlock reduce(MatrixBlock value1, MatrixBlock value2) throws Exception {

            //create correction block (on demand)
            if (_corr == null) {
                _corr = new MatrixBlock(value1.getNumRows(), value1.getNumColumns(), false);
            }

            //copy one input to output
            MatrixBlock out = new MatrixBlock(value1);

            //aggregate other input
            OperationsOnMatrixValues.incrementalAggregation(out, _corr, value2, _op, false);

            return out;
        }
    }

    private static class MergeBlocksFunction implements ReduceFunction<Tuple2<MatrixIndexes, MatrixBlock>> {
        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> reduce(Tuple2<MatrixIndexes, MatrixBlock> t1,
                                                         Tuple2<MatrixIndexes, MatrixBlock> t2) throws Exception {
            final MatrixBlock b1 = t1.f1;
            final MatrixBlock b2 = t2.f1;

            // sanity check input dimensions
            if (b1.getNumRows() != b2.getNumRows() || b1.getNumColumns() != b2.getNumColumns()) {
                throw new DMLRuntimeException("Mismatched block sizes for: "
                        + b1.getNumRows() + " " + b1.getNumColumns() + " "
                        + b2.getNumRows() + " " + b2.getNumColumns());
            }

            // execute merge (never pass by reference)
            MatrixBlock ret = new MatrixBlock(b1);
            ret.merge(b2, false);
            ret.examSparsity();

            // sanity check output number of non-zeros
            if (ret.getNonZeros() != b1.getNonZeros() + b2.getNonZeros()) {
                throw new DMLRuntimeException("Number of non-zeros does not match: "
                        + ret.getNonZeros() + " != " + b1.getNonZeros() + " + " + b2.getNonZeros());
            }

            return new Tuple2<MatrixIndexes, MatrixBlock>(t1.f0, ret);
        }
    }


    /**
     * Utility to compute number of non-zeros from the given RDD of MatrixBlocks
     *
     * @param input
     * @return
     */
    public static long computeNNZFromBlocks(DataSet<Tuple2<MatrixIndexes, MatrixBlock>> input) {
        try {
            return (long) input.map(new ComputeBinaryBlockNnzFunction()).sum(0).collect().get(0).f0.longValue();
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }
}
