package org.apache.sysml.test.integration.functions.flink;

import static org.junit.Assert.assertEquals;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetConverterUtils;
import org.apache.sysml.runtime.instructions.flink.utils.MatrixBlockIterator;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.junit.Assert;
import org.junit.Test;
import java.util.HashSet;
import java.util.List;


public class MatrixBlockIteratorTest  {
	
	public MatrixBlock generateMatrix(int x, int y) throws DMLRuntimeException {
		double [] mArray = generateArray(x,y);
		return generateMatrix(x,y,mArray);
	}

	public MatrixBlock generateMatrix(int x, int y, double [] mArray) throws DMLRuntimeException {
		MatrixBlock m = new MatrixBlock(x, y, false);

		m.init(mArray, x, y);

		return m;
	}
	
	public double [] generateArray(int x, int y) {
		double [] mArray = new double [x * y];
		int xi, yi;

		for (int c = 1; c <= x * y; c++) {
			mArray[c-1] = c;
		}
		return mArray;
	}
	
	public void runForMultipleSplitSizes(MatrixBlock src, int brlen, int bclen, int x, int y) throws Exception {
		testSplitting(new MatrixBlockIterator(src, brlen, bclen), 1, x, y);
		testSplitting(new MatrixBlockIterator(src, brlen, bclen), 2, x, y);
		testSplitting(new MatrixBlockIterator(src, brlen, bclen), 3, x, y);
		testSplitting(new MatrixBlockIterator(src, brlen, bclen), 5, x, y);
		testSplitting(new MatrixBlockIterator(src, brlen, bclen), 7, x, y);
		testSplitting(new MatrixBlockIterator(src, brlen, bclen), 10, x, y);
		//testSplitting(new MatrixBlockIterator(src, brlen, bclen), 100, x, y);
		//testSplitting(new MatrixBlockIterator(src, brlen, bclen), 1000, x, y);
	}
	

	@Test
	public void testSplitFittingBlock() throws Exception { // check
		int x = 10;
		int y = 10;
		double [] mArray = generateArray(x,y);
		MatrixBlock src = generateMatrix(x,y);
		int brlen = 2;
		int bclen = 2;
		runForMultipleSplitSizes(src, brlen, bclen, x, y);
	}

	@Test
	public void testSplitFittingBlock2() throws Exception { //check
		int x = 10;
		int y = 10;
		double [] mArray = generateArray(x,y);
		MatrixBlock src = generateMatrix(x,y);
		int brlen = 2;
		int bclen = 5;
		runForMultipleSplitSizes(src, brlen, bclen, x, y);
	}
	
	@Test
	public void testSplitNonFittingBlock() throws Exception { //check
		int x = 10;
		int y = 10;
		double [] mArray = generateArray(x,y);
		MatrixBlock src = generateMatrix(x,y);
		int brlen = 3;
		int bclen = 7;
		runForMultipleSplitSizes(src, brlen, bclen, x, y);
	}

	@Test
	public void testSplitOtherMatrix() throws Exception {
		int x = 7;
		int y = 3;
		double [] mArray = generateArray(x,y);
		MatrixBlock src = generateMatrix(x,y);
		int brlen = 2;
		int bclen = 4;
		runForMultipleSplitSizes(src, brlen, bclen, x, y);
	}

	@Test
	public void testSplitBlockGreaterMatrix() throws Exception { //check
		int x = 7;
		int y = 3;
		double [] mArray = generateArray(x,y);
		MatrixBlock src = generateMatrix(x,y);
		int brlen = 11;
		int bclen = 13;
		runForMultipleSplitSizes(src, brlen, bclen, x, y);
	}

	/*
	@Test
	public void testSplitBigMatrix() throws Exception {
		int x = 1000;
		int y = 1000;
		double [] mArray = generateArray(x,y);
		MatrixBlock src = generateMatrix(x,y);
		int brlen = 300;
		int bclen = 300;
		runForMultipleSplitSizes(src, brlen, bclen, x, y);
	}*/
	
	private static void testWithFlink(MatrixBlockIterator iter, int parallelism, int x, int y) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		TupleTypeInfo<Tuple2<MatrixIndexes, MatrixBlock>> t = new TupleTypeInfo<Tuple2<MatrixIndexes, MatrixBlock>>
			(TypeInformation.of(MatrixIndexes.class), TypeInformation.of(MatrixBlock.class));

		DataSet<Tuple2<MatrixIndexes, MatrixBlock>> matrix = env.fromParallelCollection(iter, t);
		List<MatrixBlock> l = matrix.map(new DataSetConverterUtils.ExtractElement(1)).returns(MatrixBlock.class).collect();
		HashSet<Double> reconstruction = new HashSet<Double>();
		for (MatrixBlock b : l) {
			runBlock(b,reconstruction);
		}
		Assert.assertTrue("Some elements got lost: " + reconstruction.size() + " < " + (x*y), reconstruction.size() == x*y); //check whether we got the whole range
	}
	
	private static void runBlock(MatrixBlock b, HashSet<Double> reconstruction) {
		//System.out.println("index: " + b);
		for (int r = 0; r < b.getNumRows(); r++) {
			for (int c = 0; c < b.getNumColumns(); c++) {
				//System.out.println("r: " + r + " c: " + c + " = " + b.getValue(r,c));
				Assert.assertTrue("Duplicate found: " + b.getValue(r,c), reconstruction.add(b.getValue(r,c)));
			}
		}
	}

	private static final void testSplitting(MatrixBlockIterator iter, int numSplits, int x, int y) throws Exception {
		//Flink Splitting
		testWithFlink(iter, numSplits, x, y);
		
		//Native Splitting
		MatrixBlockIterator[] splits = iter.split(numSplits);

		assertEquals(numSplits, splits.length);
		
		HashSet<Double> reconstruction = new HashSet<Double>();

		for (MatrixBlockIterator i : splits) {
			System.out.println("Iterator: " +  i);
			while (i.hasNext()) {
				Tuple2<MatrixIndexes, MatrixBlock> t = i.next();

				runBlock(t.f1, reconstruction);
			}
		}
		
		Assert.assertTrue("Some elements got lost: " + reconstruction.size() + " < " + (x*y), reconstruction.size() == x*y); //check whether we got the whole range
	}

}
