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

package org.apache.sysml.runtime.instructions.flink;



import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.flink.functions.MatrixMultiplicationFunctions;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetAggregateUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.TripleIndexes;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;

/**
 * 
 */
public class RmmFLInstruction extends BinaryFLInstruction 
{
	
	public RmmFLInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr )
	{
		super(op, in1, in2, out, opcode, istr);
		_fltype = FLINSTRUCTION_TYPE.RMM;
	}

	/**
	 * 
	 * @param str
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static RmmFLInstruction parseInstruction(String str ) 
		throws DMLRuntimeException 
	{
		String parts[] = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];

		if ( "rmm".equals(opcode) ) {
			CPOperand in1 = new CPOperand(parts[1]);
			CPOperand in2 = new CPOperand(parts[2]);
			CPOperand out = new CPOperand(parts[3]);
			
			return new RmmFLInstruction(null, in1, in2, out, opcode, str);
		} 
		else {
			throw new DMLRuntimeException("RmmSPInstruction.parseInstruction():: Unknown opcode " + opcode);
		}		
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) 
		throws DMLRuntimeException
	{
		FlinkExecutionContext flec = (FlinkExecutionContext)ec;
		
		//get input rdds
		MatrixCharacteristics mc1 = flec.getMatrixCharacteristics( input1.getName() );
		MatrixCharacteristics mc2 = flec.getMatrixCharacteristics( input2.getName() );
		DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in1 = flec.getBinaryBlockDataSetHandleForVariable( input1.getName() );
		DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in2 = flec.getBinaryBlockDataSetHandleForVariable( input2.getName() );
		
		//execute Spark RMM instruction
		//step 1: prepare join keys (w/ replication), i/j/k 
		DataSet<Tuple2<TripleIndexes,MatrixBlock>> tmp1 = in1.flatMap(
				new RmmReplicateFunction(mc2.getCols(), mc2.getColsPerBlock(), true)); 
		DataSet<Tuple2<TripleIndexes,MatrixBlock>> tmp2 = in2.flatMap(
				new RmmReplicateFunction(mc1.getRows(), mc1.getRowsPerBlock(), false));
		
		//step 2: join prepared datasets, multiply, and aggregate
		DataSet<Tuple2<MatrixIndexes,MatrixBlock>> out = 
				tmp1.join( tmp2 ).where(0).equalTo(0)                              //join by result block 
		            .with( new RmmMultiplyFunction() );   //do matrix multiplication
		
		out = out.groupBy(new MatrixMultiplicationFunctions.MatrixIndexesSelector())  //aggregation per result block
			.reduce(new MatrixMultiplicationFunctions.SumMatrixBlocksStable());
		
		//put output block into symbol table (no lineage because single block)
		updateBinaryMMOutputMatrixCharacteristics(flec, true);
		flec.setDataSetHandleForVariable(output.getName(), out);
		flec.addLineageDataSet(output.getName(), input1.getName());
		flec.addLineageDataSet(output.getName(), input2.getName());
	}


	/**
	 * 
	 */
	private static class RmmReplicateFunction implements FlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<TripleIndexes, MatrixBlock>>
	{
		private static final long serialVersionUID = 3577072668341033932L;
		
		private long _len = -1;
		private long _blen = -1;
		private boolean _left = false;
		
		public RmmReplicateFunction(long len, long blen, boolean left)
		{
			_len = len;
			_blen = blen;
			_left = left;
		}
		
		@Override
		public void flatMap(Tuple2<MatrixIndexes, MatrixBlock> arg0, Collector<Tuple2<TripleIndexes, MatrixBlock>> out) 
			throws Exception 
		{
			MatrixIndexes ixIn = arg0.f0;
			MatrixBlock blkIn = arg0.f1;
			
			long numBlocks = (long) Math.ceil((double)_len/_blen); 
			
			if( _left ) //LHS MATRIX
			{
				//replicate wrt # column blocks in RHS
				long i = ixIn.getRowIndex();
				long k = ixIn.getColumnIndex();
				for( long j=1; j<=numBlocks; j++ ) {
					TripleIndexes tmptix = new TripleIndexes(i, j, k);
					MatrixBlock tmpblk = new MatrixBlock(blkIn);
					out.collect( new Tuple2<TripleIndexes, MatrixBlock>(tmptix, tmpblk) );
				}
			} 
			else // RHS MATRIX
			{
				//replicate wrt # row blocks in LHS
				long k = ixIn.getRowIndex();
				long j = ixIn.getColumnIndex();
				for( long i=1; i<=numBlocks; i++ ) {
					TripleIndexes tmptix = new TripleIndexes(i, j, k);
					MatrixBlock tmpblk = new MatrixBlock(blkIn);
					out.collect( new Tuple2<TripleIndexes, MatrixBlock>(tmptix, tmpblk) );
				}
			}
		}
	}

	/**
	 * 
	 * 
	 */
	private static class RmmMultiplyFunction implements JoinFunction<Tuple2<TripleIndexes,MatrixBlock>, Tuple2<TripleIndexes,MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>>
	{
		private static final long serialVersionUID = -5772410117511730911L;
		
		private AggregateBinaryOperator _op = null;
		
		public RmmMultiplyFunction()
		{
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}

		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> join( Tuple2<TripleIndexes,MatrixBlock> a, Tuple2<TripleIndexes,MatrixBlock> b) throws DMLRuntimeException {
			//get input blocks per
			TripleIndexes ixIn = a.f0; //i,j,k
			MatrixIndexes ixOut = new MatrixIndexes(ixIn.getFirstIndex(), ixIn.getSecondIndex()); //i,j
			MatrixBlock blkIn1 = a.f1;
			MatrixBlock blkIn2 = b.f1;
			MatrixBlock blkOut = new MatrixBlock();
			
			//core block matrix multiplication 
			blkIn1.aggregateBinaryOperations(blkIn1, blkIn2, blkOut, _op);
							
			//output new tuple
			return new Tuple2<MatrixIndexes, MatrixBlock>(ixOut, blkOut);
		}
	}
}
