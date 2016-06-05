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


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.SplittableIterator;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

/**
 * The {@code MatrixBlockIterator} is an iterator that returns a sequence of matrix blocks (as {@code Tuple2<MatrixIndexes, MatrixBlock>})s.
 * The iterator is splittable (as defined by {@link SplittableIterator}, i.e., it can be divided into multiple
 * iterators that each return a subsequence of matrix blocks.
 */
public class MatrixBlockIterator extends SplittableIterator<Tuple2<MatrixIndexes, MatrixBlock>> {

	private static final long serialVersionUID = 1L;

	private MatrixBlock src;
	private int brlen;
	private int bclen;
	private boolean sparse;
	
	private int blockRow;
	private int blockCol;
	
	private int columnSize;
	
	private int blockRowMax;
	private int blockColMax;

	public MatrixBlockIterator(MatrixBlock src, int brlen, int bclen) {
		this.src = src;
		this.brlen = brlen;
		this.bclen = bclen;
		this.sparse = src.isInSparseFormat();

		columnSize = (int) Math.ceil(src.getNumColumns() / (double) bclen);

		this.blockRow = 0;
		this.blockRowMax = (int) Math.ceil(src.getNumRows() / (double) brlen) - 1;

		this.blockCol = 0;
		this.blockColMax = columnSize - 1;
		
		System.out.println("hello its me");
	}

	public MatrixBlockIterator(MatrixBlock src, int brlen, int bclen, int blockRow, int blockRowMax, int blockCol, int blockColMax) {
		this.src = src;
		this.brlen = brlen;
		this.bclen = bclen;
		this.sparse = src.isInSparseFormat();
		
		this.blockRow = blockRow;
		this.blockRowMax = blockRowMax;
		
		this.blockCol = blockCol;
		this.blockColMax = blockColMax;
		
		columnSize = (int) Math.ceil(src.getNumColumns() / (double) bclen);
	}

	@Override
	public boolean hasNext() {
		//either there are still rows to iterate on or we are in the last row 
		//and check whether we reached the last column already
		return ((blockRow < blockRowMax) || (blockRow == blockRowMax && blockCol <= blockColMax));
	}

	@Override
	public Tuple2<MatrixIndexes, MatrixBlock> next() {
		int maxRow = (blockRow * brlen + brlen < src.getNumRows()) ? brlen : src.getNumRows() - blockRow * brlen;
		int maxCol = (blockCol * bclen + bclen < src.getNumColumns()) ? bclen : src.getNumColumns() - blockCol * bclen;

		MatrixBlock block = new MatrixBlock(maxRow, maxCol, sparse);

		int row_offset = blockRow * brlen;
		int col_offset = blockCol * bclen;

		//copy submatrix to block
		try {
			src.sliceOperations(row_offset, row_offset + maxRow - 1,
				col_offset, col_offset + maxCol - 1, block);
		} catch (DMLRuntimeException e) {
			e.printStackTrace();
		}

		//append block to sequence file
		MatrixIndexes indexes = new MatrixIndexes(blockRow + 1, blockCol + 1);
		
		//update indices
		blockCol++;
		if (blockCol == columnSize) {
			blockCol = 0;
			blockRow++;
		}
		
		return(new Tuple2<MatrixIndexes, MatrixBlock>(indexes, block));
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	
	private void copyMatrixRange (MatrixBlock dest, int rowStart, int rowEnd, int colStart, int colEnd) {
		int c = colStart * bclen;
		double srcVal;
		
		int rStart = (rowStart * brlen);
		int rEnd = (rowEnd * brlen + brlen - 1);
		int cEnd = (colEnd * bclen + bclen - 1);
		
		if (rEnd > src.getMaxRow() - 1) {
			rEnd = src.getMaxRow() - 1;
		}
		if (cEnd > src.getMaxColumn() - 1) {
			cEnd = src.getMaxColumn() - 1;
		}
		
		for (int r = rStart; r <= rEnd; r++) {
			while ((r < rowEnd * brlen && c < src.getMaxColumn() || (r <= rEnd && c <= cEnd))) {
				srcVal = src.getValue(r,c);
				//System.out.println("r: " + r + " c: " + c + " val: " + srcVal);
				if (srcVal != 0.0) {
					dest.setValue(r, c, srcVal);
				}
				c+=1;
			}
			if (r < (rStart + brlen - 1)) {
				c = colStart * bclen;
			} else {
				c = 0;
			}
		}
	}
	
	private MatrixBlock generateBlock(int blockRowCur, int r, int blockColCur, int c) {
		MatrixBlock newBlock = new MatrixBlock(src.getNumRows(), src.getNumColumns(), true);
		try {
			copyMatrixRange (newBlock, blockRowCur, r, blockColCur, c);
			//System.out.println(newBlock);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return newBlock;
	}

	@Override
	public MatrixBlockIterator[] split(int numPartitions) {
		if (numPartitions < 1) {
			throw new IllegalArgumentException("The number of partitions must be at least 1.");
		}

		if (numPartitions == 1) {
			return new MatrixBlockIterator[]{this};
		}
		
		// here, numPartitions >= 2 !!!

		int rSize = src.getMaxRow();
		int cSize = src.getMaxColumn();
		System.out.println("rowsize: " + rSize);
		System.out.println("colsize: " + cSize);

		System.out.println("br: " + brlen);
		System.out.println("bc: " + bclen);
		
		int rBlocks = rSize / brlen;
		if (rSize % brlen > 0) {
			rBlocks++;
		}
		System.out.println("rblocks: " + rBlocks);
		
		
		int cBlocks = cSize / bclen;
		if (cSize % bclen > 0) {
			cBlocks++;
		}
		System.out.println("cblocks: " + cBlocks);
		
		int blocksPerSplit = (rBlocks * cBlocks) / numPartitions;
		if ((rBlocks * cBlocks) % numPartitions > 0) {
			blocksPerSplit++;
		}
		
		
		System.out.println("blocks per split: " + blocksPerSplit);

		MatrixBlockIterator[] iters = new MatrixBlockIterator[numPartitions];

		int b = 0;
		int i = 0;

		int blockRowCur = 0;
		int blockColCur = 0;
		
		for (int r = 0; r < rBlocks; r++) {
			for (int c = 0; c < cBlocks; c++) {
				if (b == 0) {
					blockRowCur = r;
					blockColCur = c;
				}
				
				b++;
				if (b == blocksPerSplit)  {
					b = 0;

					MatrixBlock newBlock =  generateBlock(blockRowCur, r, blockColCur, c);
					
					iters[i] = new MatrixBlockIterator(newBlock, brlen, bclen, blockRowCur, r, blockColCur, c);
					System.out.println("new iterator: row: " + blockRowCur + " - " + r + " column: " + blockColCur + " - " + c);
					i++;
				}
			}
		}

		//generate half empty iterator
		if (b > 0) {
			MatrixBlock newBlock = generateBlock(blockRowCur, rBlocks - 1, blockColCur, cBlocks - 1);
			iters[i] = new MatrixBlockIterator(newBlock, brlen, bclen, blockRowCur, rBlocks - 1, blockColCur, cBlocks - 1);
			System.out.println("new iterator: row: " + blockRowCur + " - " + (rBlocks - 1) + " column: " + blockColCur + " - " + (cBlocks - 1));
			i++;
		}
		
		//generate empty iterators
		for (; i < numPartitions; i++) {
			iters[i] = new MatrixBlockIterator(new MatrixBlock(), brlen, bclen, 0, -1, 0, -1);
		}

		return iters;
	}


	@Override
	public int getMaximumNumberOfSplits() {
		return ((blockRowMax - blockRow + 1) * columnSize);
	}


	public int getBlockRowMax() {
		return blockRowMax;
	}

	public int getBlockRow() {
		return blockRow;
	}

	public int getBlockCol() {
		return blockCol;
	}

	public int getBlockColMax() {
		return blockColMax;
	}

	public int getBclen() {
		return bclen;
	}

	public int getBrlen() {
		return brlen;
	}
}

