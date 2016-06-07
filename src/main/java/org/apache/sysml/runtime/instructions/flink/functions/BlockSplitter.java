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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

public class BlockSplitter
		implements FlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>> {

	private int brlen;
	private int bclen;

	public BlockSplitter() {
		
	}

	public BlockSplitter(int brlen, int bclen) {
		this.brlen = brlen;
		this.bclen = bclen;
	}

	@Override
	public void flatMap(Tuple2<MatrixIndexes, MatrixBlock> arg0, Collector<Tuple2<MatrixIndexes, MatrixBlock>> out)
			throws Exception {
		MatrixBlock src = arg0.f1;
		
		if (src.getNumRows() <= brlen
			&& src.getNumColumns() <= bclen) {
			out.collect(new Tuple2<MatrixIndexes, MatrixBlock>(new MatrixIndexes(1, 1), src));
		} else {
			boolean sparse = src.isInSparseFormat();

			//create and write subblocks of matrix
			for (int blockRow = 0; blockRow < (int) Math.ceil(src.getNumRows() / (double) brlen); blockRow++)
				for (int blockCol = 0; blockCol < (int) Math.ceil(src.getNumColumns() / (double) bclen); blockCol++) {
					int maxRow = (blockRow * brlen + brlen < src.getNumRows()) ? brlen : src.getNumRows() - blockRow * brlen;
					int maxCol = (blockCol * bclen + bclen < src.getNumColumns()) ? bclen : src.getNumColumns() - blockCol * bclen;

					MatrixBlock block = new MatrixBlock(maxRow, maxCol, sparse);

					int row_offset = blockRow * brlen;
					int col_offset = blockCol * bclen;

					//copy submatrix to block
					src.sliceOperations(row_offset, row_offset + maxRow - 1,
						col_offset, col_offset + maxCol - 1, block);

					//append block to sequence file
					MatrixIndexes indexes = new MatrixIndexes(blockRow + 1, blockCol + 1);
					out.collect(new Tuple2<MatrixIndexes, MatrixBlock>(indexes, block));
				}
		}
	}
}
