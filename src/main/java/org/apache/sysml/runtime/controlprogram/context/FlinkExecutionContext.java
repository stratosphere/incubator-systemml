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

package org.apache.sysml.runtime.controlprogram.context;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.Program;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.instructions.flink.data.DataSetObject;
import org.apache.sysml.runtime.instructions.flink.functions.CopyBinaryCellFunction;
import org.apache.sysml.runtime.instructions.flink.functions.CopyBlockPairFunction;
import org.apache.sysml.runtime.instructions.flink.functions.CopyTextInputFunction;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetAggregateUtils;
import org.apache.sysml.runtime.instructions.flink.utils.IOUtils;
import org.apache.sysml.runtime.matrix.data.*;
import org.apache.sysml.utils.Statistics;

import java.util.LinkedList;
import java.util.List;

public class FlinkExecutionContext extends ExecutionContext {

	//internal configurations 
	private static boolean LAZY_SPARKCTX_CREATION = true;
	private static boolean ASYNCHRONOUS_VAR_DESTROY = true;
	private static boolean FAIR_SCHEDULER_MODE = true;

	//executor memory and relative fractions as obtained from the spark configuration
	private static long _memExecutors = -1; //mem per executors
	private static double _memRatioData = -1;
	private static double _memRatioShuffle = -1;
	private static int _numExecutors = -1; //total executors
	private static int _defaultPar = -1; //total vcores  
	private static boolean _confOnly = false; //infrastructure info based on config

    private static final Log LOG = LogFactory.getLog(FlinkExecutionContext.class.getName());

    private static ExecutionEnvironment _execEnv = null;

    protected FlinkExecutionContext(Program prog) {
        this(true, prog);
    }

    protected FlinkExecutionContext(boolean allocateVars, Program prog) {
        super(allocateVars, prog);

        //if (OptimizerUtils.isHybridExecutionMode())
        initFlinkContext();
    }

    public ExecutionEnvironment getFlinkContext() {
        return _execEnv;
    }

    public DataSet<Tuple2<MatrixIndexes, MatrixBlock>> getBinaryBlockDataSetHandleForVariable(String varname)
            throws DMLRuntimeException {

        return (DataSet<Tuple2<MatrixIndexes, MatrixBlock>>) getDataSetHandleForVariable(varname,
                InputInfo.BinaryBlockInputInfo);
    }

    public DataSet<?> getDataSetHandleForVariable(String varname, InputInfo inputInfo)
            throws DMLRuntimeException {

        MatrixObject mo = getMatrixObject(varname);
        return getDataSetHandleForMatrixObject(mo, inputInfo);
    }

    public void setDataSetHandleForVariable(String varname,
                                            DataSet<Tuple2<MatrixIndexes, MatrixBlock>> ds) throws DMLRuntimeException {
        MatrixObject mo = getMatrixObject(varname);
        DataSetObject dsHandle = new DataSetObject(ds, varname);
        mo.setDataSetHandle(dsHandle);
    }

    public void addLineageDataSet(String varParent, String varChild) throws DMLRuntimeException {
        DataSetObject parent = getMatrixObject(varParent).getDataSetHandle();
        DataSetObject child = getMatrixObject(varChild).getDataSetHandle();

        parent.addLineageChild(child);
    }


    private DataSet<?> getDataSetHandleForMatrixObject(MatrixObject mo, InputInfo inputInfo)
            throws DMLRuntimeException {

        //FIXME this logic should be in matrix-object (see spark version of this method for more info)
        DataSet<?> dataSet = null;

        //CASE 1: rdd already existing (reuse if checkpoint or trigger
        //pending rdd operations if not yet cached but prevent to re-evaluate
        //rdd operations if already executed and cached
        if (mo.getDataSetHandle() != null
                && (mo.getDataSetHandle().isCheckpointed() || !mo.isCached(false))) {
            //return existing rdd handling (w/o input format change)
            dataSet = mo.getDataSetHandle().getDataSet();
        }
        //CASE 2: dirty in memory data or cached result of rdd operations
        else if (mo.isDirty() || mo.isCached(false)) {
            //get in-memory matrix block and parallelize it
            //w/ guarded parallelize (fallback to export, rdd from file if too large)
            boolean fromFile = false;
            // TODO (see spark case for large matrices)

            //default case
            MatrixBlock mb = mo.acquireRead(); //pin matrix in memory
            dataSet = toDataSet(getFlinkContext(), mb, (int) mo.getNumRowsPerBlock(), (int) mo.getNumColumnsPerBlock());
            mo.release(); //unpin matrix


            //keep rdd handle for future operations on it
            DataSetObject dshandle = new DataSetObject(dataSet, mo.getVarName());
            dshandle.setHDFSFile(fromFile);
            mo.setDataSetHandle(dshandle);
        }
        //CASE 3: non-dirty (file exists on HDFS)
        else {

            // parallelize hdfs-resident file
            // For binary block, these are: SequenceFileInputFormat.class, MatrixIndexes.class, MatrixBlock.class
            if (inputInfo == InputInfo.BinaryBlockInputInfo) {
                dataSet = IOUtils.hadoopFile(getFlinkContext(), mo.getFileName(), inputInfo.inputFormatClass,
                        inputInfo.inputKeyClass, inputInfo.inputValueClass);
                //note: this copy is still required in Spark 1.4 because spark hands out whatever the inputformat
                //recordreader returns; the javadoc explicitly recommend to copy all key/value pairs
                dataSet = ((DataSet<Tuple2<MatrixIndexes, MatrixBlock>>) dataSet).map(
                        new CopyBlockPairFunction()); //cp is workaround for read bug
            } else if (inputInfo == InputInfo.TextCellInputInfo || inputInfo == InputInfo.CSVInputInfo || inputInfo == InputInfo.MatrixMarketInputInfo) {
                dataSet = IOUtils.hadoopFile(getFlinkContext(), mo.getFileName(), inputInfo.inputFormatClass,
                        inputInfo.inputKeyClass, inputInfo.inputValueClass);
                dataSet = ((DataSet<Tuple2<LongWritable, Text>>) dataSet).map(
                        new CopyTextInputFunction()); //cp is workaround for read bug
            } else if (inputInfo == InputInfo.BinaryCellInputInfo) {
                dataSet = IOUtils.hadoopFile(getFlinkContext(), mo.getFileName(), inputInfo.inputFormatClass,
                        inputInfo.inputKeyClass, inputInfo.inputValueClass);
                dataSet = ((DataSet<Tuple2<MatrixIndexes, MatrixCell>>) dataSet).map(
                        new CopyBinaryCellFunction()); //cp is workaround for read bug
            } else {
                throw new DMLRuntimeException("Incorrect input format in getRDDHandleForVariable");
            }

            //keep dataset handle for future operations on it
            DataSetObject dataSetHandle = new DataSetObject(dataSet, mo.getVarName());
            dataSetHandle.setHDFSFile(true);
            mo.setDataSetHandle(dataSetHandle);
        }
        return dataSet;
    }

    private synchronized static void initFlinkContext() {
        _execEnv = ExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * Utility method for creating an RDD out of an in-memory matrix block.
     *
     * @param env
     * @param src
     * @return
     * @throws DMLRuntimeException
     */
    public static DataSet<Tuple2<MatrixIndexes, MatrixBlock>> toDataSet(ExecutionEnvironment env, MatrixBlock src,
                                                                        int brlen, int bclen)
            throws DMLRuntimeException {
        LinkedList<Tuple2<MatrixIndexes, MatrixBlock>> list = new LinkedList<Tuple2<MatrixIndexes, MatrixBlock>>();

        if (src.getNumRows() <= brlen
                && src.getNumColumns() <= bclen) {
            list.addLast(new Tuple2<MatrixIndexes, MatrixBlock>(new MatrixIndexes(1, 1), src));
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
                    list.addLast(new Tuple2<MatrixIndexes, MatrixBlock>(indexes, block));
                }
        }

        return env.fromCollection(list);
    }

    /**
     * @param dataset
     * @param oinfo
     */
    @SuppressWarnings("unchecked")
    public static long writeDataSetToHDFS(DataSetObject dataset, String path, OutputInfo oinfo) {
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> ldataset = (DataSet<Tuple2<MatrixIndexes, MatrixBlock>>) dataset.getDataSet();

        //recompute nnz
        long nnz = DataSetAggregateUtils.computeNNZFromBlocks(ldataset);

        //save file is an action which also triggers nnz maintenance
        IOUtils.saveAsHadoopFile(ldataset,
                path,
                oinfo.outputKeyClass,
                oinfo.outputValueClass,
                oinfo.outputFormatClass);

        //return nnz aggregate of all blocks
        return nnz;
    }

    /**
     * This method is a generic abstraction for calls from the buffer pool.
     * See toMatrixBlock(JavaPairRDD<MatrixIndexes,MatrixBlock> rdd, int numRows, int numCols);
     *
     * @param dataset
     * @param numRows
     * @param numCols
     * @return
     * @throws DMLRuntimeException
     */
    @SuppressWarnings("unchecked")
    public static MatrixBlock toMatrixBlock(DataSetObject dataset, int rlen, int clen, int brlen, int bclen, long nnz)
            throws DMLRuntimeException {
        return toMatrixBlock(
                (DataSet<Tuple2<MatrixIndexes, MatrixBlock>>) dataset.getDataSet(),
                rlen, clen, brlen, bclen, nnz);
    }

    /**
     * Utility method for creating a single matrix block out of a binary block RDD.
     * Note that this collect call might trigger execution of any pending transformations.
     * <p>
     * NOTE: This is an unguarded utility function, which requires memory for both the output matrix
     * and its collected, blocked representation.
     *
     * @param dataSet
     * @param numRows
     * @param numCols
     * @return
     * @throws DMLRuntimeException
     */
    public static MatrixBlock toMatrixBlock(DataSet<Tuple2<MatrixIndexes, MatrixBlock>> dataSet, int rlen, int clen,
                                            int brlen, int bclen, long nnz)
            throws DMLRuntimeException {

        long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;

        MatrixBlock out = null;

        if (rlen <= brlen && clen <= bclen) //SINGLE BLOCK
        {
            //special case without copy and nnz maintenance
            List<Tuple2<MatrixIndexes, MatrixBlock>> list = null;
            try {
                list = dataSet.collect();
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (list.size() > 1)
                throw new DMLRuntimeException("Expecting no more than one result block.");
            else if (list.size() == 1)
                out = list.get(0).f1;
            else //empty (e.g., after ops w/ outputEmpty=false)
                out = new MatrixBlock(rlen, clen, true);
        } else //MULTIPLE BLOCKS
        {
            //determine target sparse/dense representation
            long lnnz = (nnz >= 0) ? nnz : (long) rlen * clen;
            boolean sparse = MatrixBlock.evalSparseFormatInMemory(rlen, clen, lnnz);

            //create output matrix block (w/ lazy allocation)
            out = new MatrixBlock(rlen, clen, sparse);

            List<Tuple2<MatrixIndexes, MatrixBlock>> list = null;
            try {
                list = dataSet.collect();
            } catch (Exception e) {
                e.printStackTrace();
            }

            //copy blocks one-at-a-time into output matrix block
            for (Tuple2<MatrixIndexes, MatrixBlock> keyval : list) {
                //unpack index-block pair
                MatrixIndexes ix = keyval.f0;
                MatrixBlock block = keyval.f1;

                //compute row/column block offsets
                int row_offset = (int) (ix.getRowIndex() - 1) * brlen;
                int col_offset = (int) (ix.getColumnIndex() - 1) * bclen;
                int rows = block.getNumRows();
                int cols = block.getNumColumns();

                if (sparse) { //SPARSE OUTPUT
                    //append block to sparse target in order to avoid shifting
                    //note: this append requires a final sort of sparse rows
                    out.appendToSparse(block, row_offset, col_offset);
                } else { //DENSE OUTPUT
                    out.copy(row_offset, row_offset + rows - 1,
                            col_offset, col_offset + cols - 1, block, false);
                }
            }

            //post-processing output matrix
            if (sparse)
                out.sortSparseRows();
            out.recomputeNonZeros();
            out.examSparsity();
        }

        if (DMLScript.STATISTICS) {
            Statistics.accSparkCollectTime(System.nanoTime() - t0);
            Statistics.incSparkCollectCount(1);
        }

        return out;
    }

    @SuppressWarnings("unchecked")
    public static MatrixBlock toMatrixBlock(DataSetObject dataset, int rlen, int clen, long nnz)
            throws DMLRuntimeException {
        return toMatrixBlock(
                (DataSet<Tuple2<MatrixIndexes, MatrixCell>>) dataset.getDataSet(),
                rlen, clen, nnz);
    }

    /**
     * Utility method for creating a single matrix block out of a binary cell RDD.
     * Note that this collect call might trigger execution of any pending transformations.
     *
     * @param dataset
     * @param rlen
     * @param clen
     * @param nnz
     * @return
     * @throws DMLRuntimeException
     */
    public static MatrixBlock toMatrixBlock(DataSet<Tuple2<MatrixIndexes, MatrixCell>> dataset, int rlen, int clen,
                                            long nnz)
            throws DMLRuntimeException {
        long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;

        MatrixBlock out = null;

        //determine target sparse/dense representation
        long lnnz = (nnz >= 0) ? nnz : (long) rlen * clen;
        boolean sparse = MatrixBlock.evalSparseFormatInMemory(rlen, clen, lnnz);

        //create output matrix block (w/ lazy allocation)
        out = new MatrixBlock(rlen, clen, sparse);

        List<Tuple2<MatrixIndexes, MatrixCell>> list = null;
        try {
            list = dataset.collect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //copy blocks one-at-a-time into output matrix block
        for (Tuple2<MatrixIndexes, MatrixCell> keyval : list) {
            //unpack index-block pair
            MatrixIndexes ix = keyval.f0;
            MatrixCell cell = keyval.f1;

            //append cell to dense/sparse target in order to avoid shifting for sparse
            //note: this append requires a final sort of sparse rows
            out.appendValue((int) ix.getRowIndex() - 1, (int) ix.getColumnIndex() - 1, cell.getValue());
        }

        //post-processing output matrix
        if (sparse)
            out.sortSparseRows();
        out.recomputeNonZeros();
        out.examSparsity();

        if (DMLScript.STATISTICS) {
            Statistics.accSparkCollectTime(System.nanoTime() - t0);
            Statistics.incSparkCollectCount(1);
        }

        return out;
    }


	/**
	 * Returns the available memory budget for broadcast variables in bytes.
	 * In detail, this takes into account the total executor memory as well
	 * as relative ratios for data and shuffle. Note, that this is a conservative
	 * estimate since both data memory and shuffle memory might not be fully
	 * utilized. 
	 *
	 * @return
	 */
	public static double getBroadcastMemoryBudget()
	{
		/*
		if( _memExecutors < 0 || _memRatioData < 0 || _memRatioShuffle < 0 )
			analyzeSparkConfiguation();
			*/

		//70% of remaining free memory
		double membudget = OptimizerUtils.MEM_UTIL_FACTOR *
			(  _memExecutors
				- _memExecutors*(_memRatioData+_memRatioShuffle) );

		return membudget;
	}


	/**
	 *
	 */
	/*
	public static void analyzeSparkConfiguation()
	{
		SparkConf sconf = new SparkConf();

		//parse absolute executor memory
		String tmp = sconf.get("spark.executor.memory", "512m");
		if ( tmp.endsWith("g") || tmp.endsWith("G") )
			_memExecutors = Long.parseLong(tmp.substring(0,tmp.length()-1)) * 1024 * 1024 * 1024;
		else if ( tmp.endsWith("m") || tmp.endsWith("M") )
			_memExecutors = Long.parseLong(tmp.substring(0,tmp.length()-1)) * 1024 * 1024;
		else if( tmp.endsWith("k") || tmp.endsWith("K") )
			_memExecutors = Long.parseLong(tmp.substring(0,tmp.length()-1)) * 1024;
		else
			_memExecutors = Long.parseLong(tmp.substring(0,tmp.length()-2));

		//get data and shuffle memory ratios (defaults not specified in job conf)
		_memRatioData = sconf.getDouble("spark.storage.memoryFraction", 0.6); //default 60%
		_memRatioShuffle = sconf.getDouble("spark.shuffle.memoryFraction", 0.2); //default 20%

		int numExecutors = sconf.getInt("spark.executor.instances", -1);
		int numCoresPerExec = sconf.getInt("spark.executor.cores", -1);
		int defaultPar = sconf.getInt("spark.default.parallelism", -1);

		if( numExecutors > 1 && (defaultPar > 1 || numCoresPerExec > 1) ) {
			_numExecutors = numExecutors;
			_defaultPar = (defaultPar>1) ? defaultPar : numExecutors * numCoresPerExec;
			_confOnly = true;
		}
		else {
			//get default parallelism (total number of executors and cores)
			//note: spark context provides this information while conf does not
			//(for num executors we need to correct for driver and local mode)
			JavaSparkContext jsc = getSparkContextStatic();
			_numExecutors = Math.max(jsc.sc().getExecutorMemoryStatus().size() - 1, 1);
			_defaultPar = jsc.defaultParallelism();
			_confOnly = false; //implies env info refresh w/ spark context 
		}

		//note: required time for infrastructure analysis on 5 node cluster: ~5-20ms. 
	}*/

}
