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



import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.functionobjects.SwapIndex;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetAggregateUtils;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.ReorgOperator;


public class ZipmmFLInstruction extends BinaryFLInstruction 
{
	//internal flag to apply left-transpose rewrite or not
	private boolean _tRewrite = true;
	
	public ZipmmFLInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, boolean tRewrite, String opcode, String istr )
	{
		super(op, in1, in2, out, opcode, istr);
		_fltype = FLINSTRUCTION_TYPE.ZIPMM;
		
		_tRewrite = tRewrite;
	}
	
	/**
	 * 
	 * @param str
	 * @return
	 * @throws DMLRuntimeException
	 */
	public static ZipmmFLInstruction parseInstruction(String str ) 
		throws DMLRuntimeException 
	{
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];

		if ( opcode.equalsIgnoreCase("zipmm")) {
			CPOperand in1 = new CPOperand(parts[1]);
			CPOperand in2 = new CPOperand(parts[2]);
			CPOperand out = new CPOperand(parts[3]);
			boolean tRewrite = Boolean.parseBoolean(parts[4]);
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			AggregateBinaryOperator aggbin = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
			
			return new ZipmmFLInstruction(aggbin, in1, in2, out, tRewrite, opcode, str);
		} 
		else {
			throw new DMLRuntimeException("ZipmmSPInstruction.parseInstruction():: Unknown opcode " + opcode);
		}
		
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) 
		throws DMLRuntimeException
	{	
		FlinkExecutionContext flec = (FlinkExecutionContext)ec;
		
		//get rdd inputs (for computing r = t(X)%*%y via r = t(t(y)%*%X))
		DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in1 = flec.getBinaryBlockDataSetHandleForVariable( input1.getName() ); //X
		DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in2 = flec.getBinaryBlockDataSetHandleForVariable( input2.getName() ); //y
		
		//process core zipmm matrix multiply (in contrast to cpmm, the join over original indexes
		//preserves the original partitioning and with that potentially unnecessary join shuffle)
		DataSet<MatrixBlock> out = in1
				   .join(in2)                                       // join over original indexes
					.where(0).equalTo(0)
				   .with(new ZipMultiplyFunction(_tRewrite));  // compute block multiplications, incl t(y)
				   
		//single-block aggregation (guaranteed by zipmm blocksize constraint)
		//TODO: check wether is really necessary to break the pipeline here
		MatrixBlock out2 = DataSetAggregateUtils.sumStable(out);
		
		
		//final transpose of result (for t(t(y)%*%X))), if transpose rewrite
		if( _tRewrite ) {
			ReorgOperator rop = new ReorgOperator(SwapIndex.getSwapIndexFnObject());
			out2 = (MatrixBlock)out2.reorgOperations(rop, new MatrixBlock(), 0, 0, 0);
		}
		
		//put output block into symbol table (no lineage because single block)
		//this also includes implicit maintenance of matrix characteristics
		flec.setMatrixOutput(output.getName(), out2);
	}

	/**
	 * 
	 *
	 */
	private static class ZipMultiplyFunction implements JoinFunction<Tuple2<MatrixIndexes,MatrixBlock>, Tuple2<MatrixIndexes,MatrixBlock>, MatrixBlock> 
	{
		private static final long serialVersionUID = -6669267794926220287L;
		
		private AggregateBinaryOperator _abop = null;
		private ReorgOperator _rop = null;
		private boolean _tRewrite = true;
		
		public ZipMultiplyFunction(boolean tRewrite)
		{
			_tRewrite = tRewrite;
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_abop = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
			_rop = new ReorgOperator(SwapIndex.getSwapIndexFnObject());
		}

		@Override
		public MatrixBlock join(Tuple2<MatrixIndexes,MatrixBlock> a, Tuple2<MatrixIndexes,MatrixBlock> b)
			throws Exception 
		{
			MatrixBlock in1 = _tRewrite ? a.f1 : b.f1;
			MatrixBlock in2 = _tRewrite ? b.f1 : a.f1;
			
			//transpose right input (for vectors no-op)
			MatrixBlock tmp = (MatrixBlock)in2.reorgOperations(_rop, new MatrixBlock(), 0, 0, 0);
				
			//core matrix multiplication (for t(y)%*%X or t(X)%*%y)
			return (MatrixBlock)tmp.aggregateBinaryOperations(tmp, in1, new MatrixBlock(), _abop);
		}
	}
}
