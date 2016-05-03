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

package org.apache.sysml.runtime.instructions.flink.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.BinaryOperator;

import java.util.Map;

public class OuterVectorBinaryOpFunction extends RichFlatMapBroadcastFunction<Tuple2<MatrixIndexes,MatrixBlock>, Tuple2<MatrixIndexes,MatrixBlock>>
{
	private static final long serialVersionUID = 1730704346934726826L;
	
	private BinaryOperator _op;
	
	public OuterVectorBinaryOpFunction(BinaryOperator op) 
	{
		_op = op;
	}

	@Override
	public void flatMap(Tuple2<MatrixIndexes, MatrixBlock> arg0, Collector<Tuple2<MatrixIndexes, MatrixBlock>> out)
		throws Exception
	{
		MatrixBlock resultBlk = null;
		
		for(Map.Entry<Long,MatrixBlock> in2 : _pbc.get(1L).entrySet())
		{
			resultBlk = (MatrixBlock)arg0.f1.binaryOperations (_op, in2.getValue(), new MatrixBlock());
			resultBlk.examSparsity();
			out.collect(new Tuple2<MatrixIndexes,MatrixBlock>(new MatrixIndexes(arg0.f0.getRowIndex(), in2.getKey()), resultBlk));
		}
	}
}
