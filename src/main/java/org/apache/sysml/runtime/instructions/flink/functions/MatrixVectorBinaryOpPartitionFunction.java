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


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.sysml.lops.BinaryM.VectorType;
import org.apache.sysml.runtime.instructions.spark.data.LazyIterableIterator;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.BinaryOperator;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MatrixVectorBinaryOpPartitionFunction extends RichMapFunction<Tuple2<MatrixIndexes,MatrixBlock>, Tuple2<MatrixIndexes,MatrixBlock>>
{
	private static final long serialVersionUID = 9096091404578628534L;
	
	private BinaryOperator _op = null;
	private HashMap<Long, HashMap<Long, MatrixBlock>> _pmV = null;
	private VectorType _vtype = null;
	
	public MatrixVectorBinaryOpPartitionFunction(BinaryOperator op, VectorType vtype ) 
	{
		_op = op;
		_vtype = vtype;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		_pmV = new HashMap<Long, HashMap<Long, MatrixBlock>>();

		Collection<org.apache.flink.api.java.tuple.Tuple2<MatrixIndexes,MatrixBlock>> blocklist = getRuntimeContext().getBroadcastVariable("bcastVar");

		for (org.apache.flink.api.java.tuple.Tuple2<MatrixIndexes,MatrixBlock> broadcastTuple : blocklist){
			long columnIndex = broadcastTuple.f0.getColumnIndex();
			long rowIndex = broadcastTuple.f0.getRowIndex();

			HashMap<Long, MatrixBlock> tempMap = _pmV.get(rowIndex);
			if (tempMap == null) {
				tempMap = new HashMap<Long, MatrixBlock>();
			}
			tempMap.put(columnIndex, broadcastTuple.f1);
			_pmV.put(rowIndex, tempMap);
		}
	}

	public void close() throws Exception {
		for (Map.Entry<Long,HashMap<Long,MatrixBlock>> e : _pmV.entrySet()) {
			e.getValue().clear();
		}
		_pmV.clear();
	}

	@Override
	public Tuple2<MatrixIndexes, MatrixBlock> map(Tuple2<MatrixIndexes, MatrixBlock> arg0)
		throws Exception 
	{
		//get the rhs block 
		long rix= ((_vtype==VectorType.COL_VECTOR) ? arg0.f0.getRowIndex() : 1L);
		long cix= ((_vtype==VectorType.COL_VECTOR) ? 1L : arg0.f0.getColumnIndex());
		MatrixBlock in2 = _pmV.get(rix).get(cix);

		//execute the binary operation
		MatrixBlock ret = (MatrixBlock) (arg0.f1.binaryOperations (_op, in2, new MatrixBlock()));
		return(new Tuple2<MatrixIndexes, MatrixBlock>(arg0.f0, ret));
	}
}
