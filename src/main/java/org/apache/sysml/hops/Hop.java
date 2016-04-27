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

package org.apache.sysml.hops;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.api.DMLScript.RUNTIME_PLATFORM;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.lops.CSVReBlock;
import org.apache.sysml.lops.Checkpoint;
import org.apache.sysml.lops.Data;
import org.apache.sysml.lops.Lop;
import org.apache.sysml.lops.LopsException;
import org.apache.sysml.lops.ReBlock;
import org.apache.sysml.lops.UnaryCP;
import org.apache.sysml.lops.LopProperties.ExecType;
import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.controlprogram.LocalVariableMap;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.controlprogram.parfor.ProgramConverter;
import org.apache.sysml.runtime.controlprogram.parfor.util.IDSequence;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.util.UtilFunctions;


public abstract class Hop 
{
	
	protected static final Log LOG =  LogFactory.getLog(Hop.class.getName());
	
	public static final long CPThreshold = 2000;
	protected static final boolean BREAKONSCALARS = false;
	protected static final boolean SPLITLARGEMATRIXMULT = true;

	public enum VisitStatus {
		DONE, 
		VISITING, 
		NOTVISITED,
	}
	
	/**
	 * Optional hop interface, to be implemented by multi-threaded hops.
	 */
	public interface MultiThreadedHop {
		public abstract void setMaxNumThreads( int k );
		public abstract int getMaxNumThreads();
	}

	// static variable to assign an unique ID to every hop that is created
	private static IDSequence _seqHopID = new IDSequence();
	
	protected long _ID;
	protected String _name;
	protected DataType _dataType;
	protected ValueType _valueType;
	protected VisitStatus _visited = VisitStatus.NOTVISITED;
	protected long _dim1 = -1;
	protected long _dim2 = -1;
	protected long _rows_in_block = -1;
	protected long _cols_in_block = -1;
	protected long _nnz = -1;
	protected boolean _updateInPlace = false;

	protected ArrayList<Hop> _parent = new ArrayList<Hop>();
	protected ArrayList<Hop> _input = new ArrayList<Hop>();

	protected ExecType _etype = null; //currently used exec type
	protected ExecType _etypeForced = null; //exec type forced via platform or external optimizer
	
	// Estimated size for the output produced from this Hop
	protected double _outputMemEstimate = OptimizerUtils.INVALID_SIZE;
	
	// Estimated size for the entire operation represented by this Hop
	// It includes the memory required for all inputs as well as the output 
	protected double _memEstimate = OptimizerUtils.INVALID_SIZE;
	protected double _processingMemEstimate = 0;
	protected double _spBroadcastMemEstimate = 0;
	
	// indicates if there are unknowns during compilation 
	// (in that case re-complication ensures robustness and efficiency)
	protected boolean _requiresRecompile = false;
	
	// indicates if the output of this hop needs to be reblocked
	// (usually this happens on persistent reads dataops)
	protected boolean _requiresReblock = false;
	
	// indicates if the output of this hop needs to be checkpointed (cached)
	// (the default storage level for caching is not yet exposed here)
	protected boolean _requiresCheckpoint = false;
	
	// indicates if the output of this hops needs to contain materialized empty blocks 
	// if those exists; otherwise only blocks w/ non-zero values are materialized
	protected boolean _outputEmptyBlocks = true;
	
	private Lop _lops = null;
	
	protected Hop(){
		//default constructor for clone
	}
		
	public Hop(String l, DataType dt, ValueType vt) {
		_ID = getNextHopID();
		setName(l);
		setDataType(dt);
		setValueType(vt);
	}

	
	private static long getNextHopID() {
		return _seqHopID.getNextID();
	}
	
	public long getHopID() {
		return _ID;
	}
	
	public ExecType getExecType()
	{
		return _etype;
	}
	
	public void resetExecType()
	{
		_etype = null;
	}
	
	/**
	 * 
	 * @return
	 */
	public ExecType getForcedExecType()
	{
		return _etypeForced;
	}
	
	/**
	 * 
	 * @param etype
	 */
	public void setForcedExecType(ExecType etype)
	{
		_etypeForced = etype;
	}
	
	/**
	 * 
	 * @return
	 */
	public abstract boolean allowsAllExecTypes();
	
	/**
	 * Defines if this operation is transpose-safe, which means that
	 * the result of op(input) is equivalent to op(t(input)).
	 * Usually, this applies to aggregate operations with fixed output
	 * dimension. Finally, this information is very useful in order to
	 * safely optimize the plan for sparse vectors, which otherwise
	 * would be (currently) always represented dense.
	 * 
	 * 
	 * @return
	 */
	public boolean isTransposeSafe()
	{
		//by default: its conservatively define as unsafe
		return false;
	}
	
	/**
	 * 
	 */
	public void checkAndSetForcedPlatform()
	{
		if ( DMLScript.rtplatform == RUNTIME_PLATFORM.SINGLE_NODE )
			_etypeForced = ExecType.CP;
		else if ( DMLScript.rtplatform == RUNTIME_PLATFORM.HADOOP )
			_etypeForced = ExecType.MR;
		else if ( DMLScript.rtplatform == RUNTIME_PLATFORM.SPARK )
			_etypeForced = ExecType.SPARK;
		else if ( DMLScript.rtplatform == RUNTIME_PLATFORM.FLINK )
			_etypeForced = ExecType.FLINK;
	}
	
	/**
	 * 
	 */
	public void checkAndSetInvalidCPDimsAndSize()
	{		
		if( _etype == ExecType.CP )
		{
			boolean invalid = false;
			
			//Step 1: check dimensions of output and all inputs (INTEGER)
			invalid |= !OptimizerUtils.isValidCPDimensions(_dim1, _dim2);
			for( Hop in : getInput() )
				invalid |= !OptimizerUtils.isValidCPDimensions(in._dim1, in._dim2);
			
			//Step 2: check valid output and input sizes for cp (<16GB for DENSE)
			//(if the memory estimate is smaller than max_numcells we are guaranteed to have it in sparse representation)
			invalid |= !(  OptimizerUtils.isValidCPMatrixSize(_dim1, _dim2, OptimizerUtils.getSparsity(_dim1, _dim2, _nnz))
					    || getOutputMemEstimate() < OptimizerUtils.MAX_NUMCELLS_CP_DENSE );
			for( Hop in : getInput() )
				invalid |= !(   OptimizerUtils.isValidCPMatrixSize(in._dim1, in._dim2, OptimizerUtils.getSparsity(in._dim1, in._dim2, in._nnz))
						     || in.getOutputMemEstimate() < OptimizerUtils.MAX_NUMCELLS_CP_DENSE);
			
			//force exec type mr if necessary
			if( invalid ) { 
				if( DMLScript.rtplatform == RUNTIME_PLATFORM.HYBRID )
					_etype = ExecType.MR;
				else if( DMLScript.rtplatform == RUNTIME_PLATFORM.HYBRID_SPARK )
					_etype = ExecType.SPARK;
				else if( DMLScript.rtplatform == RUNTIME_PLATFORM.HYBRID_FLINK)
					_etype = ExecType.FLINK;
			}
		}
	}
	
	public void setRequiresReblock(boolean flag)
	{
		_requiresReblock = flag;
	}
	
	public boolean hasMatrixInputWithDifferentBlocksizes()
	{
		for( Hop c : getInput() ) {
			if(    c.getDataType()==DataType.MATRIX
			    &&(getRowsInBlock() != c.getRowsInBlock()
			    || getColsInBlock() != c.getColsInBlock()) )
			{
				return true;
			}
		}
		
		return false;
	}
	
	public void setOutputBlocksizes( long brlen, long bclen )
	{
		setRowsInBlock( brlen );
		setColsInBlock( bclen );
	}
	
	public boolean requiresReblock()
	{
		return _requiresReblock;
	}
	
	public void setRequiresCheckpoint(boolean flag)
	{
		_requiresCheckpoint = flag;
	}
	
	public boolean requiresCheckpoint()
	{
		return _requiresCheckpoint;
	}
	
	
	/**
	 * 
	 * @throws HopsException
	 */
	public void constructAndSetLopsDataFlowProperties() 
		throws HopsException
	{
		//Step 1: construct reblock lop if required (output of hop)
		constructAndSetReblockLopIfRequired();
		
		//Step 2: construct checkpoint lop if required (output of hop or reblock)
		constructAndSetCheckpointLopIfRequired();
	}
	
	/**
	 * 
	 * @throws HopsException
	 */
	private void constructAndSetReblockLopIfRequired() 
		throws HopsException
	{
		//determine execution type
		ExecType et = ExecType.CP;
		if( DMLScript.rtplatform != RUNTIME_PLATFORM.SINGLE_NODE 
			&& !(getDataType()==DataType.SCALAR) )
		{
			et = OptimizerUtils.getRemoteExecType();
		}

		//add reblock lop to output if required
		if( _requiresReblock && et != ExecType.CP )
		{
			Lop input = getLops();
			Lop reblock = null;
			
			try
			{
				if(    (this instanceof DataOp  // CSV
							&& ((DataOp)this).getDataOpType() == DataOpTypes.PERSISTENTREAD
							&& ((DataOp)this).getInputFormatType() == FileFormatTypes.CSV ) 
					|| (this instanceof ParameterizedBuiltinOp 
							&& ((ParameterizedBuiltinOp)this).getOp() == ParamBuiltinOp.TRANSFORM) )
				{
					reblock = new CSVReBlock( input, getRowsInBlock(), getColsInBlock(), 
							getDataType(), getValueType(), et);
				}
				else //TEXT / MM / BINARYBLOCK / BINARYCELL  
				{
					reblock = new ReBlock( input, getRowsInBlock(), getColsInBlock(), 
		                    getDataType(), getValueType(), _outputEmptyBlocks, et);
				}
			}
			catch( LopsException ex ) {
				throw new HopsException(ex);
			}
		
			setOutputDimensions( reblock );
			setLineNumbers( reblock );
			setLops( reblock );
		}
	}
	
	/**
	 * 
	 * @throws HopsException
	 */
	@SuppressWarnings("unused") //see CHECKPOINT_SPARSE_CSR
	private void constructAndSetCheckpointLopIfRequired() 
		throws HopsException
	{
		//determine execution type
		ExecType et = ExecType.CP;
		if (OptimizerUtils.isSparkExecutionMode() && getDataType() != DataType.SCALAR)
		{
			//conditional checkpoint based on memory estimate in order to 
			//(1) avoid unnecessary persist and unpersist calls, and 
			//(2) avoid unnecessary creation of spark context (incl executors)
			if(    OptimizerUtils.isHybridExecutionMode() 
				&& (getDim2() > 1 && _outputMemEstimate < OptimizerUtils.getLocalMemBudget()
				|| getDim2() == 1 && _outputMemEstimate < OptimizerUtils.getLocalMemBudget()/3 )
				|| _etypeForced == ExecType.CP )
			{
				et = ExecType.CP;
			}
			else //default case
			{
				et = ExecType.SPARK;
			}
		}

		//add checkpoint lop to output if required
		if( _requiresCheckpoint && et != ExecType.CP )
		{
			try
			{
				//investigate need for serialized storage of large sparse matrices
				//(compile- instead of runtime-level for better debugging)
				boolean serializedStorage = false;
				if( dimsKnown(true) && !Checkpoint.CHECKPOINT_SPARSE_CSR ) {
					double matrixPSize = OptimizerUtils.estimatePartitionedSizeExactSparsity(_dim1, _dim2, _rows_in_block, _cols_in_block, _nnz);
					double dataCache = SparkExecutionContext.getConfiguredTotalDataMemory(true);
					serializedStorage = (MatrixBlock.evalSparseFormatInMemory(_dim1, _dim2, _nnz)
							             && matrixPSize > dataCache ); //sparse in-memory does not fit in agg mem 
				}
				else if( !dimsKnown(true) ) {
					setRequiresRecompile();
				}
			
				//construct checkpoint w/ right storage level
				Lop input = getLops();			
				Lop chkpoint = new Checkpoint(input, getDataType(), getValueType(), 
						serializedStorage ? Checkpoint.getSerializeStorageLevelString() :
								            Checkpoint.getDefaultStorageLevelString() );
				
				setOutputDimensions( chkpoint );
				setLineNumbers( chkpoint );
				setLops( chkpoint );
			}
			catch( LopsException ex ) {
				throw new HopsException(ex);
			}
		}
		
	}
	
	
	/**
	 * 
	 * @param inputPos
	 * @return
	 * @throws HopsException
	 * @throws LopsException
	 */
	public static Lop createOffsetLop( Hop hop, boolean repCols ) 
		throws HopsException, LopsException
	{
		Lop offset = null;
		
		if( ConfigurationManager.isDynamicRecompilation() && hop.dimsKnown() )
		{
			// If dynamic recompilation is enabled and dims are known, we can replace the ncol with 
			// a literal in order to increase the piggybacking potential. This is safe because append 
			// is always marked for recompilation and hence, we have propagated the exact dimensions.
			offset = Data.createLiteralLop(ValueType.INT, String.valueOf(repCols ? hop.getDim2() : hop.getDim1()));
		}
		else
		{
			offset = new UnaryCP(hop.constructLops(), 
					      repCols ? UnaryCP.OperationTypes.NCOL : UnaryCP.OperationTypes.NROW, 
					      DataType.SCALAR, ValueType.INT);
		}
		
		offset.getOutputParameters().setDimensions(0, 0, 0, 0, -1);
		offset.setAllPositions(hop.getBeginLine(), hop.getBeginColumn(), hop.getEndLine(), hop.getEndColumn());
		
		return offset;
	}
	
	public void setOutputEmptyBlocks(boolean flag)
	{
		_outputEmptyBlocks = flag;
	}
	
	public boolean isOutputEmptyBlocks()
	{
		return _outputEmptyBlocks;
	}
	
	/**
	 * Returns the memory estimate for the output produced from this Hop.
	 * It must be invoked only within Hops. From outside Hops, one must 
	 * only use getMemEstimate(), which gives memory required to store 
	 * all inputs and the output.
	 * 
	 * @return
	 */
	protected double getOutputSize() 
	{
		return _outputMemEstimate;
	}
	
	/**
	 * 
	 * @return
	 */
	protected double getInputSize() 
	{
		double sum = 0;		
		int len = _input.size();
		
		for( int i=0; i<len; i++ ) //for all inputs
		{
			Hop hi = _input.get(i);
			double hmout = hi.getOutputMemEstimate();
			
			if( hmout > 1024*1024 ) {//for relevant sizes
				//check if already included in estimate (if an input is used
				//multiple times it is still only required once in memory)
				//(not that this check benefits from common subexpression elimination)
				boolean flag = false;
				for( int j=0; j<i; j++ )
					flag |= (hi == _input.get(j));
				hmout = flag ? 0 : hmout;
			}
			
			sum += hmout;
		}
		
		//for(Hop h : _input ) {
		//	sum += h._outputMemEstimate;
		//}
		
		return sum;
	}
	
	/**
	 * 
	 * @return
	 */
	protected double getInputOutputSize() 
	{
		double sum = 0;
		sum += _outputMemEstimate;
		sum += _processingMemEstimate;
		sum += getInputSize();
		
		return sum;
	}
	
	/**
	 * 
	 * @param pos
	 * @return
	 */
	protected double getInputSize( int pos ){
		double ret = 0;
		if( _input.size()>pos )
			ret = _input.get(pos)._outputMemEstimate;
		return ret;
	}
	
	protected double getIntermediateSize() {
		return _processingMemEstimate;
	}
	
	/**
	 * NOTES:
	 * * Purpose: Whenever the output dimensions / sparsity of a hop are unknown, this hop
	 *   should store its worst-case output statistics (if known) in that table. Subsequent
	 *   hops can then
	 * * Invocation: Intended to be called for ALL root nodes of one Hops DAG with the same
	 *   (initially empty) memo table.
	 * 
	 * @param memo	
	 * @return
	 */
	public double getMemEstimate()
	{
		if ( OptimizerUtils.isMemoryBasedOptLevel() ) {
			if ( ! isMemEstimated() ) {
				//LOG.warn("Nonexisting memory estimate - reestimating w/o memo table.");
				computeMemEstimate( new MemoTable() ); 
			}
			return _memEstimate;
		}
		else {
			return OptimizerUtils.INVALID_SIZE;
		}
	}
	
	/**
	 * Returns memory estimate in bytes
	 * 
	 * @param mem
	 */
	public void setMemEstimate( double mem )
	{
		_memEstimate = mem;
	}
	
	public void clearMemEstimate()
	{
		_memEstimate = OptimizerUtils.INVALID_SIZE;
	}

	public boolean isMemEstimated() 
	{
		return (_memEstimate != OptimizerUtils.INVALID_SIZE);
	}

	//wrappers for meaningful public names to memory estimates.
	
	public double getInputMemEstimate()
	{
		return getInputSize();
	}
	
	public double getOutputMemEstimate()
	{
		return getOutputSize();
	}

	public double getIntermediateMemEstimate()
	{
		return getIntermediateSize();
	}
	
	public double getSpBroadcastSize()
	{
		return _spBroadcastMemEstimate;
	}
	
	/**
	 * Computes the estimate of memory required to store the input/output of this hop in memory. 
	 * This is the default implementation (orchestration of hop-specific implementation) 
	 * that should suffice for most hops. If a hop requires more control, this method should
	 * be overwritten with awareness of (1) output estimates, and (2) propagation of worst-case
	 * matrix characteristics (dimensions, sparsity).  
	 * 
	 * TODO remove memo table and, on constructor refresh, inference in refresh, single compute mem,
	 * maybe general computeMemEstimate, flags to indicate if estimate or not. 
	 * 
	 * @return computed estimate
	 */
	public void computeMemEstimate( MemoTable memo )
	{
		long[] wstats = null; 
		
		////////
		//Step 1) Compute hop output memory estimate (incl size inference) 
		
		switch( getDataType() )
		{
			case SCALAR:
			{
				//memory estimate always known
				if( getValueType()== ValueType.DOUBLE) //default case
					_outputMemEstimate = OptimizerUtils.DOUBLE_SIZE;
				else //literalops, dataops
					_outputMemEstimate = computeOutputMemEstimate( _dim1, _dim2, _nnz );
				break;
			}
			case MATRIX:
			{
				//1a) mem estimate based on exactly known dimensions and sparsity
				if( dimsKnown(true) ) { 
					//nnz always exactly known (see dimsKnown(true))
					_outputMemEstimate = computeOutputMemEstimate( _dim1, _dim2, _nnz );
				}
				//1b) infer output statistics and mem estimate based on these statistics
				else if( memo.hasInputStatistics(this) )
				{
					//infer the output stats
					wstats = inferOutputCharacteristics(memo);
					
					if( wstats != null ) {
						//use worst case characteristics to estimate mem
						long lnnz = ((wstats[2]>=0)?wstats[2]:wstats[0]*wstats[1]);
						_outputMemEstimate = computeOutputMemEstimate( wstats[0], wstats[1], lnnz );
						
						//propagate worst-case estimate
						memo.memoizeStatistics(getHopID(), wstats[0], wstats[1], wstats[2]);
					}
					else if( dimsKnown() ) {
						//nnz unknown, estimate mem as dense
						long lnnz = _dim1*_dim2;
						_outputMemEstimate = computeOutputMemEstimate( _dim1, _dim2, lnnz );
					}
					else {
						//unknown output size
						_outputMemEstimate = OptimizerUtils.DEFAULT_SIZE;
					}
				}
				//1c) mem estimate based on exactly known dimensions and unknown sparsity
				//(required e.g., for datagenops w/o any input statistics)
				else if( dimsKnown() ) {
					//nnz unknown, estimate mem as dense
					long lnnz = _dim1*_dim2;
					_outputMemEstimate = computeOutputMemEstimate( _dim1, _dim2, lnnz );
				}
				//1d) fallback: unknown output size
				else {
					_outputMemEstimate = OptimizerUtils.DEFAULT_SIZE;
				}
				
				break;
			}
			case OBJECT:
			case UNKNOWN:
			case FRAME:	
			{
				//memory estimate always unknown
				_outputMemEstimate = OptimizerUtils.DEFAULT_SIZE;
				break;
			}
		}
		
		////////
		//Step 2) Compute hop intermediate memory estimate  
		
		//note: ensure consistency w/ step 1 (for simplified debugging)	
		
		if( dimsKnown(true) ) { //incl scalar output
			//nnz always exactly known (see dimsKnown(true))
			_processingMemEstimate = computeIntermediateMemEstimate( _dim1, _dim2, _nnz );
		}
		else if( wstats!=null ) {
			//use worst case characteristics to estimate mem
			long lnnz = ((wstats[2]>=0)?wstats[2]:wstats[0]*wstats[1]);
			_processingMemEstimate = computeIntermediateMemEstimate( wstats[0], wstats[1], lnnz );
		}
		else if( dimsKnown() ){
			//nnz unknown, estimate mem as dense
			long lnnz = _dim1 * _dim2;
			_processingMemEstimate = computeIntermediateMemEstimate(_dim1, _dim2, lnnz);
		}
		
		
		////////
		//Step 3) Compute final hop memory estimate  
			
		//final estimate (sum of inputs/intermediates/output)
		_memEstimate = getInputOutputSize();
	}

	
	/**
	 * Computes the hop-specific output memory estimate in bytes. Should be 0 if not
	 * applicable. 
	 * 
	 * @param dim1
	 * @param dim2
	 * @param nnz
	 * @return
	 */
	protected abstract double computeOutputMemEstimate( long dim1, long dim2, long nnz );

	/**
	 * Computes the hop-specific intermediate memory estimate in bytes. Should be 0 if not
	 * applicable.
	 * 
	 * @param dim1
	 * @param dim2
	 * @param nnz
	 * @return
	 */
	protected abstract double computeIntermediateMemEstimate( long dim1, long dim2, long nnz );

	/**
	 * Computes the output matrix characteristics (rows, cols, nnz) based on worst-case output
	 * and/or input estimates. Should return null if dimensions are unknown.
	 * 
	 * @param memo
	 * @return
	 */
	protected abstract long[] inferOutputCharacteristics( MemoTable memo );

	
	
	/**
	 * This function is used only for sanity check.
	 * Returns true if estimates for all the hops in the DAG rooted at the current 
	 * hop are computed. Returns false if any of the hops have INVALID estimate.
	 * 
	 * @return
	 */
	public boolean checkEstimates() {
		boolean childStatus = true;
		for (Hop h : this.getInput())
			childStatus = childStatus && h.checkEstimates();
		return childStatus && (_memEstimate != OptimizerUtils.INVALID_SIZE);
	}
	
	/**
	 * Recursively computes memory estimates for all the Hops in the DAG rooted at the 
	 * current hop pointed by <code>this</code>.
	 * 
	 */
	public void refreshMemEstimates( MemoTable memo ) {
		if (getVisited() == VisitStatus.DONE)
			return;
		for (Hop h : this.getInput())
			h.refreshMemEstimates( memo );
		this.computeMemEstimate( memo );
		this.setVisited(VisitStatus.DONE);
	}

	/**
	 * This method determines the execution type (CP, MR) based ONLY on the 
	 * estimated memory footprint required for this operation, which includes 
	 * memory for all inputs and the output represented by this Hop.
	 * 
	 * It is used when <code>OptimizationType = MEMORY_BASED</code>.
	 * This optimization schedules an operation to CP whenever inputs+output 
	 * fit in memory -- note that this decision MAY NOT be optimal in terms of 
	 * execution time.
	 * 
	 * @return
	 */
	protected ExecType findExecTypeByMemEstimate() {
		ExecType et = null;
		char c = ' ';
		if ( getMemEstimate() < OptimizerUtils.getLocalMemBudget() ) {
			et = ExecType.CP;
		}
		else {
			if( DMLScript.rtplatform == DMLScript.RUNTIME_PLATFORM.HYBRID )
				et = ExecType.MR;
			else if( DMLScript.rtplatform == DMLScript.RUNTIME_PLATFORM.HYBRID_SPARK )
				et = ExecType.SPARK;
			else if( DMLScript.rtplatform == DMLScript.RUNTIME_PLATFORM.HYBRID_FLINK )
				et = ExecType.FLINK;
			
			c = '*';
		}

		if (LOG.isDebugEnabled()){
			String s = String.format("  %c %-5s %-8s (%s,%s)  %s", c, getHopID(), getOpString(), OptimizerUtils.toMB(_outputMemEstimate), OptimizerUtils.toMB(_memEstimate), et);
			//System.out.println(s);
			LOG.debug(s);
		}
		
		return et;
	}

	public ArrayList<Hop> getParent() {
		return _parent;
	}

	public ArrayList<Hop> getInput() {
		return _input;
	}

	/**
	 * Create bidirectional links
	 * 
	 * @param h
	 */
	public void addInput( Hop h )
	{
		_input.add(h);
		h._parent.add(this);
	}

	public long getRowsInBlock() {
		return _rows_in_block;
	}

	public void setRowsInBlock(long rowsInBlock) {
		_rows_in_block = rowsInBlock;
	}

	public long getColsInBlock() {
		return _cols_in_block;
	}

	public void setColsInBlock(long colsInBlock) {
		_cols_in_block = colsInBlock;
	}

	public void setNnz(long nnz){
		_nnz = nnz;
	}
	
	public long getNnz(){
		return _nnz;
	}

	public void setUpdateInPlace(boolean updateInPlace){
		_updateInPlace = updateInPlace;
	}
	
	public boolean getUpdateInPlace(){
		return _updateInPlace;
	}

	public abstract Lop constructLops() 
		throws HopsException, LopsException;

	protected abstract ExecType optFindExecType() 
		throws HopsException;
	
	public abstract String getOpString();

	protected boolean isVector() {
		return (dimsKnown() && (_dim1 == 1 || _dim2 == 1) );
	}
	
	protected boolean areDimsBelowThreshold() {
		return (dimsKnown() && _dim1 <= Hop.CPThreshold && _dim2 <= Hop.CPThreshold );
	}
	
	public boolean dimsKnown() {
		return ( _dataType == DataType.SCALAR || (_dataType==DataType.MATRIX && _dim1 > 0 && _dim2 > 0) );
	}
	
	public boolean dimsKnown(boolean includeNnz) {
		return ( _dataType == DataType.SCALAR || (_dataType==DataType.MATRIX && _dim1 > 0 && _dim2 > 0 && ((includeNnz)? _nnz>=0 : true)) );
	}

	public boolean dimsKnownAny() {
		return ( _dataType == DataType.SCALAR || (_dataType==DataType.MATRIX && (_dim1 > 0 || _dim2 > 0)) );
	}
	
	public static void resetVisitStatus( ArrayList<Hop> hops )
	{
		if( hops != null )
			for( Hop hopRoot : hops )
				hopRoot.resetVisitStatus();
	}
	
	public void resetVisitStatus() 
	{
		if ( getVisited() == Hop.VisitStatus.NOTVISITED )
			return;
		
		for (Hop h : this.getInput())
			h.resetVisitStatus();
		
		setVisited(Hop.VisitStatus.NOTVISITED);
	}

	public static void resetRecompilationFlag( ArrayList<Hop> hops, ExecType et )
	{
		resetVisitStatus( hops );
		for( Hop hopRoot : hops )
			hopRoot.resetRecompilationFlag( et );
	}
	
	public static void resetRecompilationFlag( Hop hops, ExecType et )
	{
		hops.resetVisitStatus();
		hops.resetRecompilationFlag( et );
	}
	
	private void resetRecompilationFlag( ExecType et ) 
	{
		if( getVisited() == VisitStatus.DONE )
			return;
		
		//process child hops
		for (Hop h : getInput())
			h.resetRecompilationFlag( et );
		
		//reset recompile flag
		if( et == null || getExecType() == et || getExecType()==null )
			_requiresRecompile = false;
		
		this.setVisited(VisitStatus.DONE);
	}
	
		
	/**
	 * Test and debugging only.
	 * 
	 * @throws HopsException 
	 */
	public void checkParentChildPointers( ) 
		throws HopsException
	{
		if( getVisited() == VisitStatus.DONE )
			return;
		
		for( Hop in : getInput() )
		{
			if( !in.getParent().contains(this) )
				throw new HopsException("Parent-Child pointers incorrect.");
			in.checkParentChildPointers();
		}
		
		setVisited(VisitStatus.DONE);
	}
	
	public void printMe() throws HopsException {
		if (LOG.isDebugEnabled()) {
			StringBuilder s = new StringBuilder(""); 
			s.append(getClass().getSimpleName() + " " + getHopID() + "\n");
			s.append("  Label: " + getName() + "; DataType: " + _dataType + "; ValueType: " + _valueType + "\n");
			s.append("  Parent: ");
			for (Hop h : getParent()) {
				s.append(h.hashCode() + "; ");
			}
			;
			s.append("\n  Input: ");
			for (Hop h : getInput()) {
				s.append(h.getHopID() + "; ");
			}
			
			s.append("\n  dims [" + _dim1 + "," + _dim2 + "] blk [" + _rows_in_block + "," + _cols_in_block + "] nnz: " + _nnz + " UpdateInPlace: " + _updateInPlace);
			s.append("  MemEstimate = Out " + (_outputMemEstimate/1024/1024) + " MB, In&Out " + (_memEstimate/1024/1024) + " MB" );
			LOG.debug(s.toString());
		}
	}

	public long getDim1() {
		return _dim1;
	}

	public void setDim1(long dim1) {
		_dim1 = dim1;
	}

	public long getDim2() {
		return _dim2;
	}

	public void setDim2(long dim2) {
		_dim2 = dim2;
	}
	
	protected void setOutputDimensions(Lop lop) 
		throws HopsException
	{
		lop.getOutputParameters().setDimensions(
			getDim1(), getDim2(), getRowsInBlock(), getColsInBlock(), getNnz(), getUpdateInPlace());	
	}
	
	public Lop getLops() {
		return _lops;
	}

	public void setLops(Lop lops) {
		_lops = lops;
	}

	public VisitStatus getVisited() {
		return _visited;
	}

	public DataType getDataType() {
		return _dataType;
	}
	
	public void setDataType( DataType dt ) {
		_dataType = dt;
	}

	public void setVisited(VisitStatus visited) {
		_visited = visited;
	}

	public void setName(String _name) {
		this._name = _name;
	}

	public String getName() {
		return _name;
	}

	public ValueType getValueType() {
		return _valueType;
	}
	
	public void setValueType(ValueType vt) {
		_valueType = vt;
	}

	public enum OpOp1 {
		NOT, ABS, SIN, COS, TAN, ASIN, ACOS, ATAN, SIGN, SQRT, LOG, EXP, 
		CAST_AS_SCALAR, CAST_AS_MATRIX, CAST_AS_FRAME, CAST_AS_DOUBLE, CAST_AS_INT, CAST_AS_BOOLEAN, 
		PRINT, EIGEN, NROW, NCOL, LENGTH, ROUND, IQM, STOP, CEIL, FLOOR, MEDIAN, INVERSE, CHOLESKY,
		//cumulative sums, products, extreme values
		CUMSUM, CUMPROD, CUMMIN, CUMMAX,
		//fused ML-specific operators for performance 
		SPROP, //sample proportion: P * (1 - P)
		SIGMOID, //sigmoid function: 1 / (1 + exp(-X)) 
		SELP, //select positive: X * (X>0)
		LOG_NZ, //sparse-safe log; ppred(X,0,"!=")*log(X)
	}

	// Operations that require two operands
	public enum OpOp2 {
		PLUS, MINUS, MULT, DIV, MODULUS, INTDIV, LESS, LESSEQUAL, GREATER, GREATEREQUAL, EQUAL, NOTEQUAL, 
		MIN, MAX, AND, OR, LOG, POW, PRINT, CONCAT, QUANTILE, INTERQUANTILE, IQM, 
		CENTRALMOMENT, COVARIANCE, CBIND, RBIND, SOLVE, MEDIAN, INVALID,
		//fused ML-specific operators for performance
		MINUS_NZ, //sparse-safe minus: X-(mean*ppred(X,0,!=))
		LOG_NZ, //sparse-safe log; ppred(X,0,"!=")*log(X,0.5)
		MINUS1_MULT, //1-X*Y
	};

	// Operations that require 3 operands
	public enum OpOp3 {
		QUANTILE, INTERQUANTILE, CTABLE, CENTRALMOMENT, COVARIANCE, INVALID 
	};
	
	// Operations that require 4 operands
	public enum OpOp4 {
		WSLOSS, //weighted sloss mm
		WSIGMOID, //weighted sigmoid mm
		WDIVMM, //weighted divide mm
		WCEMM, //weighted cross entropy mm
		WUMM, //weighted unary mm 
		INVALID 
	};
	
	
	public enum AggOp {
		SUM, SUM_SQ, MIN, MAX, TRACE, PROD, MEAN, VAR, MAXINDEX, MININDEX
	};

	public enum ReOrgOp {
		TRANSPOSE, DIAG, RESHAPE, SORT, REV
		//Note: Diag types are invalid because for unknown sizes this would 
		//create incorrect plans (now we try to infer it for memory estimates
		//and rewrites but the final choice is made during runtime)
		//DIAG_V2M, DIAG_M2V, 
	};
	
	public enum DataGenMethod {
		RAND, SEQ, SINIT, SAMPLE, INVALID
	};

	public enum ParamBuiltinOp {
		INVALID, CDF, INVCDF, GROUPEDAGG, RMEMPTY, REPLACE, REXPAND, 
		TRANSFORM, TRANSFORMAPPLY, TRANSFORMDECODE, TRANSFORMMETA,
	};

	/**
	 * Functions that are built in, but whose execution takes place in an
	 * external library
	 */
	public enum ExtBuiltInOp {
		EIGEN, CHOLESKY
	};

	public enum FileFormatTypes {
		TEXT, BINARY, MM, CSV
	};

	public enum DataOpTypes {
		PERSISTENTREAD, PERSISTENTWRITE, TRANSIENTREAD, TRANSIENTWRITE, FUNCTIONOUTPUT
	};

	public enum Direction {
		RowCol, Row, Col
	};

	protected static final HashMap<DataOpTypes, org.apache.sysml.lops.Data.OperationTypes> HopsData2Lops;
	static {
		HopsData2Lops = new HashMap<Hop.DataOpTypes, org.apache.sysml.lops.Data.OperationTypes>();
		HopsData2Lops.put(DataOpTypes.PERSISTENTREAD, org.apache.sysml.lops.Data.OperationTypes.READ);
		HopsData2Lops.put(DataOpTypes.PERSISTENTWRITE, org.apache.sysml.lops.Data.OperationTypes.WRITE);
		HopsData2Lops.put(DataOpTypes.TRANSIENTWRITE, org.apache.sysml.lops.Data.OperationTypes.WRITE);
		HopsData2Lops.put(DataOpTypes.TRANSIENTREAD, org.apache.sysml.lops.Data.OperationTypes.READ);
	}

	protected static final HashMap<Hop.AggOp, org.apache.sysml.lops.Aggregate.OperationTypes> HopsAgg2Lops;
	static {
		HopsAgg2Lops = new HashMap<Hop.AggOp, org.apache.sysml.lops.Aggregate.OperationTypes>();
		HopsAgg2Lops.put(AggOp.SUM, org.apache.sysml.lops.Aggregate.OperationTypes.KahanSum);
		HopsAgg2Lops.put(AggOp.SUM_SQ, org.apache.sysml.lops.Aggregate.OperationTypes.KahanSumSq);
		HopsAgg2Lops.put(AggOp.TRACE, org.apache.sysml.lops.Aggregate.OperationTypes.KahanTrace);
		HopsAgg2Lops.put(AggOp.MIN, org.apache.sysml.lops.Aggregate.OperationTypes.Min);
		HopsAgg2Lops.put(AggOp.MAX, org.apache.sysml.lops.Aggregate.OperationTypes.Max);
		HopsAgg2Lops.put(AggOp.MAXINDEX, org.apache.sysml.lops.Aggregate.OperationTypes.MaxIndex);
		HopsAgg2Lops.put(AggOp.MININDEX, org.apache.sysml.lops.Aggregate.OperationTypes.MinIndex);
		HopsAgg2Lops.put(AggOp.PROD, org.apache.sysml.lops.Aggregate.OperationTypes.Product);
		HopsAgg2Lops.put(AggOp.MEAN, org.apache.sysml.lops.Aggregate.OperationTypes.Mean);
		HopsAgg2Lops.put(AggOp.VAR, org.apache.sysml.lops.Aggregate.OperationTypes.Var);
	}

	protected static final HashMap<ReOrgOp, org.apache.sysml.lops.Transform.OperationTypes> HopsTransf2Lops;
	static {
		HopsTransf2Lops = new HashMap<ReOrgOp, org.apache.sysml.lops.Transform.OperationTypes>();
		HopsTransf2Lops.put(ReOrgOp.TRANSPOSE, org.apache.sysml.lops.Transform.OperationTypes.Transpose);
		HopsTransf2Lops.put(ReOrgOp.REV, org.apache.sysml.lops.Transform.OperationTypes.Rev);
		HopsTransf2Lops.put(ReOrgOp.DIAG, org.apache.sysml.lops.Transform.OperationTypes.Diag);
		HopsTransf2Lops.put(ReOrgOp.RESHAPE, org.apache.sysml.lops.Transform.OperationTypes.Reshape);
		HopsTransf2Lops.put(ReOrgOp.SORT, org.apache.sysml.lops.Transform.OperationTypes.Sort);

	}

	protected static final HashMap<Hop.Direction, org.apache.sysml.lops.PartialAggregate.DirectionTypes> HopsDirection2Lops;
	static {
		HopsDirection2Lops = new HashMap<Hop.Direction, org.apache.sysml.lops.PartialAggregate.DirectionTypes>();
		HopsDirection2Lops.put(Direction.RowCol, org.apache.sysml.lops.PartialAggregate.DirectionTypes.RowCol);
		HopsDirection2Lops.put(Direction.Col, org.apache.sysml.lops.PartialAggregate.DirectionTypes.Col);
		HopsDirection2Lops.put(Direction.Row, org.apache.sysml.lops.PartialAggregate.DirectionTypes.Row);

	}

	protected static final HashMap<Hop.OpOp2, org.apache.sysml.lops.Binary.OperationTypes> HopsOpOp2LopsB;
	static {
		HopsOpOp2LopsB = new HashMap<Hop.OpOp2, org.apache.sysml.lops.Binary.OperationTypes>();
		HopsOpOp2LopsB.put(OpOp2.PLUS, org.apache.sysml.lops.Binary.OperationTypes.ADD);
		HopsOpOp2LopsB.put(OpOp2.MINUS, org.apache.sysml.lops.Binary.OperationTypes.SUBTRACT);
		HopsOpOp2LopsB.put(OpOp2.MULT, org.apache.sysml.lops.Binary.OperationTypes.MULTIPLY);
		HopsOpOp2LopsB.put(OpOp2.DIV, org.apache.sysml.lops.Binary.OperationTypes.DIVIDE);
		HopsOpOp2LopsB.put(OpOp2.MODULUS, org.apache.sysml.lops.Binary.OperationTypes.MODULUS);
		HopsOpOp2LopsB.put(OpOp2.INTDIV, org.apache.sysml.lops.Binary.OperationTypes.INTDIV);
		HopsOpOp2LopsB.put(OpOp2.MINUS1_MULT, org.apache.sysml.lops.Binary.OperationTypes.MINUS1_MULTIPLY);
		HopsOpOp2LopsB.put(OpOp2.LESS, org.apache.sysml.lops.Binary.OperationTypes.LESS_THAN);
		HopsOpOp2LopsB.put(OpOp2.LESSEQUAL, org.apache.sysml.lops.Binary.OperationTypes.LESS_THAN_OR_EQUALS);
		HopsOpOp2LopsB.put(OpOp2.GREATER, org.apache.sysml.lops.Binary.OperationTypes.GREATER_THAN);
		HopsOpOp2LopsB.put(OpOp2.GREATEREQUAL, org.apache.sysml.lops.Binary.OperationTypes.GREATER_THAN_OR_EQUALS);
		HopsOpOp2LopsB.put(OpOp2.EQUAL, org.apache.sysml.lops.Binary.OperationTypes.EQUALS);
		HopsOpOp2LopsB.put(OpOp2.NOTEQUAL, org.apache.sysml.lops.Binary.OperationTypes.NOT_EQUALS);
		HopsOpOp2LopsB.put(OpOp2.MIN, org.apache.sysml.lops.Binary.OperationTypes.MIN);
		HopsOpOp2LopsB.put(OpOp2.MAX, org.apache.sysml.lops.Binary.OperationTypes.MAX);
		HopsOpOp2LopsB.put(OpOp2.AND, org.apache.sysml.lops.Binary.OperationTypes.OR);
		HopsOpOp2LopsB.put(OpOp2.OR, org.apache.sysml.lops.Binary.OperationTypes.AND);
		HopsOpOp2LopsB.put(OpOp2.SOLVE, org.apache.sysml.lops.Binary.OperationTypes.SOLVE);
		HopsOpOp2LopsB.put(OpOp2.POW, org.apache.sysml.lops.Binary.OperationTypes.POW);
		HopsOpOp2LopsB.put(OpOp2.LOG, org.apache.sysml.lops.Binary.OperationTypes.NOTSUPPORTED);
	}

	protected static final HashMap<Hop.OpOp2, org.apache.sysml.lops.BinaryScalar.OperationTypes> HopsOpOp2LopsBS;
	static {
		HopsOpOp2LopsBS = new HashMap<Hop.OpOp2, org.apache.sysml.lops.BinaryScalar.OperationTypes>();
		HopsOpOp2LopsBS.put(OpOp2.PLUS, org.apache.sysml.lops.BinaryScalar.OperationTypes.ADD);	
		HopsOpOp2LopsBS.put(OpOp2.MINUS, org.apache.sysml.lops.BinaryScalar.OperationTypes.SUBTRACT);
		HopsOpOp2LopsBS.put(OpOp2.MULT, org.apache.sysml.lops.BinaryScalar.OperationTypes.MULTIPLY);
		HopsOpOp2LopsBS.put(OpOp2.DIV, org.apache.sysml.lops.BinaryScalar.OperationTypes.DIVIDE);
		HopsOpOp2LopsBS.put(OpOp2.MODULUS, org.apache.sysml.lops.BinaryScalar.OperationTypes.MODULUS);
		HopsOpOp2LopsBS.put(OpOp2.INTDIV, org.apache.sysml.lops.BinaryScalar.OperationTypes.INTDIV);
		HopsOpOp2LopsBS.put(OpOp2.LESS, org.apache.sysml.lops.BinaryScalar.OperationTypes.LESS_THAN);
		HopsOpOp2LopsBS.put(OpOp2.LESSEQUAL, org.apache.sysml.lops.BinaryScalar.OperationTypes.LESS_THAN_OR_EQUALS);
		HopsOpOp2LopsBS.put(OpOp2.GREATER, org.apache.sysml.lops.BinaryScalar.OperationTypes.GREATER_THAN);
		HopsOpOp2LopsBS.put(OpOp2.GREATEREQUAL, org.apache.sysml.lops.BinaryScalar.OperationTypes.GREATER_THAN_OR_EQUALS);
		HopsOpOp2LopsBS.put(OpOp2.EQUAL, org.apache.sysml.lops.BinaryScalar.OperationTypes.EQUALS);
		HopsOpOp2LopsBS.put(OpOp2.NOTEQUAL, org.apache.sysml.lops.BinaryScalar.OperationTypes.NOT_EQUALS);
		HopsOpOp2LopsBS.put(OpOp2.MIN, org.apache.sysml.lops.BinaryScalar.OperationTypes.MIN);
		HopsOpOp2LopsBS.put(OpOp2.MAX, org.apache.sysml.lops.BinaryScalar.OperationTypes.MAX);
		HopsOpOp2LopsBS.put(OpOp2.AND, org.apache.sysml.lops.BinaryScalar.OperationTypes.AND);
		HopsOpOp2LopsBS.put(OpOp2.OR, org.apache.sysml.lops.BinaryScalar.OperationTypes.OR);
		HopsOpOp2LopsBS.put(OpOp2.LOG, org.apache.sysml.lops.BinaryScalar.OperationTypes.LOG);
		HopsOpOp2LopsBS.put(OpOp2.POW, org.apache.sysml.lops.BinaryScalar.OperationTypes.POW);
		HopsOpOp2LopsBS.put(OpOp2.PRINT, org.apache.sysml.lops.BinaryScalar.OperationTypes.PRINT);
	}

	protected static final HashMap<Hop.OpOp2, org.apache.sysml.lops.Unary.OperationTypes> HopsOpOp2LopsU;
	static {
		HopsOpOp2LopsU = new HashMap<Hop.OpOp2, org.apache.sysml.lops.Unary.OperationTypes>();
		HopsOpOp2LopsU.put(OpOp2.PLUS, org.apache.sysml.lops.Unary.OperationTypes.ADD);
		HopsOpOp2LopsU.put(OpOp2.MINUS, org.apache.sysml.lops.Unary.OperationTypes.SUBTRACT);
		HopsOpOp2LopsU.put(OpOp2.MULT, org.apache.sysml.lops.Unary.OperationTypes.MULTIPLY);
		HopsOpOp2LopsU.put(OpOp2.DIV, org.apache.sysml.lops.Unary.OperationTypes.DIVIDE);
		HopsOpOp2LopsU.put(OpOp2.MODULUS, org.apache.sysml.lops.Unary.OperationTypes.MODULUS);
		HopsOpOp2LopsU.put(OpOp2.INTDIV, org.apache.sysml.lops.Unary.OperationTypes.INTDIV);
		HopsOpOp2LopsU.put(OpOp2.MINUS1_MULT, org.apache.sysml.lops.Unary.OperationTypes.MINUS1_MULTIPLY);
		HopsOpOp2LopsU.put(OpOp2.LESSEQUAL, org.apache.sysml.lops.Unary.OperationTypes.LESS_THAN_OR_EQUALS);
		HopsOpOp2LopsU.put(OpOp2.LESS, org.apache.sysml.lops.Unary.OperationTypes.LESS_THAN);
		HopsOpOp2LopsU.put(OpOp2.GREATEREQUAL, org.apache.sysml.lops.Unary.OperationTypes.GREATER_THAN_OR_EQUALS);
		HopsOpOp2LopsU.put(OpOp2.GREATER, org.apache.sysml.lops.Unary.OperationTypes.GREATER_THAN);
		HopsOpOp2LopsU.put(OpOp2.EQUAL, org.apache.sysml.lops.Unary.OperationTypes.EQUALS);
		HopsOpOp2LopsU.put(OpOp2.NOTEQUAL, org.apache.sysml.lops.Unary.OperationTypes.NOT_EQUALS);
		HopsOpOp2LopsU.put(OpOp2.AND, org.apache.sysml.lops.Unary.OperationTypes.NOTSUPPORTED);
		HopsOpOp2LopsU.put(OpOp2.OR, org.apache.sysml.lops.Unary.OperationTypes.NOTSUPPORTED);
		HopsOpOp2LopsU.put(OpOp2.MAX, org.apache.sysml.lops.Unary.OperationTypes.MAX);
		HopsOpOp2LopsU.put(OpOp2.MIN, org.apache.sysml.lops.Unary.OperationTypes.MIN);
		HopsOpOp2LopsU.put(OpOp2.LOG, org.apache.sysml.lops.Unary.OperationTypes.LOG);
		HopsOpOp2LopsU.put(OpOp2.POW, org.apache.sysml.lops.Unary.OperationTypes.POW);
		HopsOpOp2LopsU.put(OpOp2.MINUS_NZ, org.apache.sysml.lops.Unary.OperationTypes.SUBTRACT_NZ);
		HopsOpOp2LopsU.put(OpOp2.LOG_NZ, org.apache.sysml.lops.Unary.OperationTypes.LOG_NZ);
	}

	protected static final HashMap<Hop.OpOp1, org.apache.sysml.lops.Unary.OperationTypes> HopsOpOp1LopsU;
	static {
		HopsOpOp1LopsU = new HashMap<Hop.OpOp1, org.apache.sysml.lops.Unary.OperationTypes>();
		HopsOpOp1LopsU.put(OpOp1.NOT, org.apache.sysml.lops.Unary.OperationTypes.NOT);
		HopsOpOp1LopsU.put(OpOp1.ABS, org.apache.sysml.lops.Unary.OperationTypes.ABS);
		HopsOpOp1LopsU.put(OpOp1.SIN, org.apache.sysml.lops.Unary.OperationTypes.SIN);
		HopsOpOp1LopsU.put(OpOp1.COS, org.apache.sysml.lops.Unary.OperationTypes.COS);
		HopsOpOp1LopsU.put(OpOp1.TAN, org.apache.sysml.lops.Unary.OperationTypes.TAN);
		HopsOpOp1LopsU.put(OpOp1.ASIN, org.apache.sysml.lops.Unary.OperationTypes.ASIN);
		HopsOpOp1LopsU.put(OpOp1.ACOS, org.apache.sysml.lops.Unary.OperationTypes.ACOS);
		HopsOpOp1LopsU.put(OpOp1.ATAN, org.apache.sysml.lops.Unary.OperationTypes.ATAN);
		HopsOpOp1LopsU.put(OpOp1.SIGN, org.apache.sysml.lops.Unary.OperationTypes.SIGN);
		HopsOpOp1LopsU.put(OpOp1.SQRT, org.apache.sysml.lops.Unary.OperationTypes.SQRT);
		HopsOpOp1LopsU.put(OpOp1.EXP, org.apache.sysml.lops.Unary.OperationTypes.EXP);
		HopsOpOp1LopsU.put(OpOp1.LOG, org.apache.sysml.lops.Unary.OperationTypes.LOG);
		HopsOpOp1LopsU.put(OpOp1.ROUND, org.apache.sysml.lops.Unary.OperationTypes.ROUND);
		HopsOpOp1LopsU.put(OpOp1.CEIL, org.apache.sysml.lops.Unary.OperationTypes.CEIL);
		HopsOpOp1LopsU.put(OpOp1.FLOOR, org.apache.sysml.lops.Unary.OperationTypes.FLOOR);
		HopsOpOp1LopsU.put(OpOp1.CUMSUM, org.apache.sysml.lops.Unary.OperationTypes.CUMSUM);
		HopsOpOp1LopsU.put(OpOp1.CUMPROD, org.apache.sysml.lops.Unary.OperationTypes.CUMPROD);
		HopsOpOp1LopsU.put(OpOp1.CUMMIN, org.apache.sysml.lops.Unary.OperationTypes.CUMMIN);
		HopsOpOp1LopsU.put(OpOp1.CUMMAX, org.apache.sysml.lops.Unary.OperationTypes.CUMMAX);
		HopsOpOp1LopsU.put(OpOp1.INVERSE, org.apache.sysml.lops.Unary.OperationTypes.INVERSE);
		HopsOpOp1LopsU.put(OpOp1.CHOLESKY, org.apache.sysml.lops.Unary.OperationTypes.CHOLESKY);
		HopsOpOp1LopsU.put(OpOp1.CAST_AS_SCALAR, org.apache.sysml.lops.Unary.OperationTypes.NOTSUPPORTED);
		HopsOpOp1LopsU.put(OpOp1.CAST_AS_MATRIX, org.apache.sysml.lops.Unary.OperationTypes.NOTSUPPORTED);
		HopsOpOp1LopsU.put(OpOp1.SPROP, org.apache.sysml.lops.Unary.OperationTypes.SPROP);
		HopsOpOp1LopsU.put(OpOp1.SIGMOID, org.apache.sysml.lops.Unary.OperationTypes.SIGMOID);
		HopsOpOp1LopsU.put(OpOp1.SELP, org.apache.sysml.lops.Unary.OperationTypes.SELP);
		HopsOpOp1LopsU.put(OpOp1.LOG_NZ, org.apache.sysml.lops.Unary.OperationTypes.LOG_NZ);
	}

	protected static final HashMap<Hop.OpOp1, org.apache.sysml.lops.UnaryCP.OperationTypes> HopsOpOp1LopsUS;
	static {
		HopsOpOp1LopsUS = new HashMap<Hop.OpOp1, org.apache.sysml.lops.UnaryCP.OperationTypes>();
		HopsOpOp1LopsUS.put(OpOp1.NOT, org.apache.sysml.lops.UnaryCP.OperationTypes.NOT);
		HopsOpOp1LopsUS.put(OpOp1.ABS, org.apache.sysml.lops.UnaryCP.OperationTypes.ABS);
		HopsOpOp1LopsUS.put(OpOp1.SIN, org.apache.sysml.lops.UnaryCP.OperationTypes.SIN);
		HopsOpOp1LopsUS.put(OpOp1.COS, org.apache.sysml.lops.UnaryCP.OperationTypes.COS);
		HopsOpOp1LopsUS.put(OpOp1.TAN, org.apache.sysml.lops.UnaryCP.OperationTypes.TAN);
		HopsOpOp1LopsUS.put(OpOp1.ASIN, org.apache.sysml.lops.UnaryCP.OperationTypes.ASIN);
		HopsOpOp1LopsUS.put(OpOp1.ACOS, org.apache.sysml.lops.UnaryCP.OperationTypes.ACOS);
		HopsOpOp1LopsUS.put(OpOp1.ATAN, org.apache.sysml.lops.UnaryCP.OperationTypes.ATAN);
		HopsOpOp1LopsUS.put(OpOp1.SQRT, org.apache.sysml.lops.UnaryCP.OperationTypes.SQRT);
		HopsOpOp1LopsUS.put(OpOp1.EXP, org.apache.sysml.lops.UnaryCP.OperationTypes.EXP);
		HopsOpOp1LopsUS.put(OpOp1.LOG, org.apache.sysml.lops.UnaryCP.OperationTypes.LOG);
		HopsOpOp1LopsUS.put(OpOp1.CAST_AS_SCALAR, org.apache.sysml.lops.UnaryCP.OperationTypes.CAST_AS_SCALAR);
		HopsOpOp1LopsUS.put(OpOp1.CAST_AS_MATRIX, org.apache.sysml.lops.UnaryCP.OperationTypes.CAST_AS_MATRIX);
		HopsOpOp1LopsUS.put(OpOp1.CAST_AS_FRAME, org.apache.sysml.lops.UnaryCP.OperationTypes.CAST_AS_FRAME);
		HopsOpOp1LopsUS.put(OpOp1.CAST_AS_DOUBLE, org.apache.sysml.lops.UnaryCP.OperationTypes.CAST_AS_DOUBLE);
		HopsOpOp1LopsUS.put(OpOp1.CAST_AS_INT, org.apache.sysml.lops.UnaryCP.OperationTypes.CAST_AS_INT);
		HopsOpOp1LopsUS.put(OpOp1.CAST_AS_BOOLEAN, org.apache.sysml.lops.UnaryCP.OperationTypes.CAST_AS_BOOLEAN);
		HopsOpOp1LopsUS.put(OpOp1.NROW, org.apache.sysml.lops.UnaryCP.OperationTypes.NROW);
		HopsOpOp1LopsUS.put(OpOp1.NCOL, org.apache.sysml.lops.UnaryCP.OperationTypes.NCOL);
		HopsOpOp1LopsUS.put(OpOp1.LENGTH, org.apache.sysml.lops.UnaryCP.OperationTypes.LENGTH);
		HopsOpOp1LopsUS.put(OpOp1.PRINT, org.apache.sysml.lops.UnaryCP.OperationTypes.PRINT);
		HopsOpOp1LopsUS.put(OpOp1.ROUND, org.apache.sysml.lops.UnaryCP.OperationTypes.ROUND);
		HopsOpOp1LopsUS.put(OpOp1.CEIL, org.apache.sysml.lops.UnaryCP.OperationTypes.CEIL);
		HopsOpOp1LopsUS.put(OpOp1.FLOOR, org.apache.sysml.lops.UnaryCP.OperationTypes.FLOOR);
		HopsOpOp1LopsUS.put(OpOp1.STOP, org.apache.sysml.lops.UnaryCP.OperationTypes.STOP);
	}

	protected static final HashMap<Hop.OpOp1, String> HopsOpOp12String;
	static {
		HopsOpOp12String = new HashMap<OpOp1, String>();	
		HopsOpOp12String.put(OpOp1.ABS, "abs");
		HopsOpOp12String.put(OpOp1.CAST_AS_SCALAR, "castAsScalar");
		HopsOpOp12String.put(OpOp1.COS, "cos");
		HopsOpOp12String.put(OpOp1.EIGEN, "eigen");
		HopsOpOp12String.put(OpOp1.EXP, "exp");
		HopsOpOp12String.put(OpOp1.IQM, "iqm");
		HopsOpOp12String.put(OpOp1.MEDIAN, "median");
		HopsOpOp12String.put(OpOp1.LENGTH, "length");
		HopsOpOp12String.put(OpOp1.LOG, "log");
		HopsOpOp12String.put(OpOp1.NCOL, "ncol");
		HopsOpOp12String.put(OpOp1.NOT, "!");
		HopsOpOp12String.put(OpOp1.NROW, "nrow");
		HopsOpOp12String.put(OpOp1.PRINT, "print");
		HopsOpOp12String.put(OpOp1.ROUND, "round");
		HopsOpOp12String.put(OpOp1.SIN, "sin");
		HopsOpOp12String.put(OpOp1.SQRT, "sqrt");
		HopsOpOp12String.put(OpOp1.TAN, "tan");
		HopsOpOp12String.put(OpOp1.ASIN, "asin");
		HopsOpOp12String.put(OpOp1.ACOS, "acos");
		HopsOpOp12String.put(OpOp1.ATAN, "atan");
		HopsOpOp12String.put(OpOp1.STOP, "stop");
		HopsOpOp12String.put(OpOp1.INVERSE, "inv");
		HopsOpOp12String.put(OpOp1.SPROP, "sprop");
		HopsOpOp12String.put(OpOp1.SIGMOID, "sigmoid");
	}
	
	protected static final HashMap<Hop.ParamBuiltinOp, org.apache.sysml.lops.ParameterizedBuiltin.OperationTypes> HopsParameterizedBuiltinLops;
	static {
		HopsParameterizedBuiltinLops = new HashMap<Hop.ParamBuiltinOp, org.apache.sysml.lops.ParameterizedBuiltin.OperationTypes>();
		HopsParameterizedBuiltinLops.put(ParamBuiltinOp.CDF, org.apache.sysml.lops.ParameterizedBuiltin.OperationTypes.CDF);
		HopsParameterizedBuiltinLops.put(ParamBuiltinOp.INVCDF, org.apache.sysml.lops.ParameterizedBuiltin.OperationTypes.INVCDF);
		HopsParameterizedBuiltinLops.put(ParamBuiltinOp.RMEMPTY, org.apache.sysml.lops.ParameterizedBuiltin.OperationTypes.RMEMPTY);
		HopsParameterizedBuiltinLops.put(ParamBuiltinOp.REPLACE, org.apache.sysml.lops.ParameterizedBuiltin.OperationTypes.REPLACE);
		HopsParameterizedBuiltinLops.put(ParamBuiltinOp.REXPAND, org.apache.sysml.lops.ParameterizedBuiltin.OperationTypes.REXPAND);
		HopsParameterizedBuiltinLops.put(ParamBuiltinOp.TRANSFORM, org.apache.sysml.lops.ParameterizedBuiltin.OperationTypes.TRANSFORM);
		HopsParameterizedBuiltinLops.put(ParamBuiltinOp.TRANSFORMAPPLY, org.apache.sysml.lops.ParameterizedBuiltin.OperationTypes.TRANSFORMAPPLY);		
		HopsParameterizedBuiltinLops.put(ParamBuiltinOp.TRANSFORMDECODE, org.apache.sysml.lops.ParameterizedBuiltin.OperationTypes.TRANSFORMDECODE);
		HopsParameterizedBuiltinLops.put(ParamBuiltinOp.TRANSFORMMETA, org.apache.sysml.lops.ParameterizedBuiltin.OperationTypes.TRANSFORMMETA);
	}

	protected static final HashMap<Hop.OpOp2, String> HopsOpOp2String;
	static {
		HopsOpOp2String = new HashMap<Hop.OpOp2, String>();
		HopsOpOp2String.put(OpOp2.PLUS, "+");
		HopsOpOp2String.put(OpOp2.MINUS, "-");
		HopsOpOp2String.put(OpOp2.MINUS_NZ, "-nz");
		HopsOpOp2String.put(OpOp2.MINUS1_MULT, "-1*");
		HopsOpOp2String.put(OpOp2.MULT, "*");
		HopsOpOp2String.put(OpOp2.DIV, "/");
		HopsOpOp2String.put(OpOp2.MODULUS, "%%");
		HopsOpOp2String.put(OpOp2.INTDIV, "%/%");
		HopsOpOp2String.put(OpOp2.MIN, "min");
		HopsOpOp2String.put(OpOp2.MAX, "max");
		HopsOpOp2String.put(OpOp2.LESSEQUAL, "<=");
		HopsOpOp2String.put(OpOp2.LESS, "<");
		HopsOpOp2String.put(OpOp2.GREATEREQUAL, ">=");
		HopsOpOp2String.put(OpOp2.GREATER, ">");
		HopsOpOp2String.put(OpOp2.EQUAL, "=");
		HopsOpOp2String.put(OpOp2.NOTEQUAL, "!=");
		HopsOpOp2String.put(OpOp2.OR, "|");
		HopsOpOp2String.put(OpOp2.AND, "&");
		HopsOpOp2String.put(OpOp2.LOG, "log");
		HopsOpOp2String.put(OpOp2.LOG_NZ, "log_nz");
		HopsOpOp2String.put(OpOp2.POW, "^");
		HopsOpOp2String.put(OpOp2.CONCAT, "concat");
		HopsOpOp2String.put(OpOp2.INVALID, "?");
		HopsOpOp2String.put(OpOp2.QUANTILE, "quantile");
		HopsOpOp2String.put(OpOp2.INTERQUANTILE, "interquantile");
		HopsOpOp2String.put(OpOp2.IQM, "IQM");
		HopsOpOp2String.put(OpOp2.MEDIAN, "median");
		HopsOpOp2String.put(OpOp2.CENTRALMOMENT, "cm");
		HopsOpOp2String.put(OpOp2.COVARIANCE, "cov");
		HopsOpOp2String.put(OpOp2.CBIND, "cbind");
		HopsOpOp2String.put(OpOp2.RBIND, "rbind");
		HopsOpOp2String.put(OpOp2.SOLVE, "solve");
	}
	
	public static String getOpOp2String( OpOp2 op ) {
		return HopsOpOp2String.get(op);
	}
	
	protected static final HashMap<Hop.OpOp3, String> HopsOpOp3String;
	static {
		HopsOpOp3String = new HashMap<Hop.OpOp3, String>();
		HopsOpOp3String.put(OpOp3.QUANTILE, "quantile");
		HopsOpOp3String.put(OpOp3.INTERQUANTILE, "interquantile");
		HopsOpOp3String.put(OpOp3.CTABLE, "ctable");
		HopsOpOp3String.put(OpOp3.CENTRALMOMENT, "cm");
		HopsOpOp3String.put(OpOp3.COVARIANCE, "cov");
	}
	
	protected static final HashMap<Hop.OpOp4, String> HopsOpOp4String;
	static {
		HopsOpOp4String = new HashMap<Hop.OpOp4, String>();
		HopsOpOp4String.put(OpOp4.WSLOSS,   "wsloss");
		HopsOpOp4String.put(OpOp4.WSIGMOID, "wsigmoid");
		HopsOpOp4String.put(OpOp4.WCEMM,    "wcemm");
		HopsOpOp4String.put(OpOp4.WDIVMM,   "wdivmm");
		HopsOpOp4String.put(OpOp4.WUMM,     "wumm");
	}

	protected static final HashMap<Hop.Direction, String> HopsDirection2String;
	static {
		HopsDirection2String = new HashMap<Hop.Direction, String>();
		HopsDirection2String.put(Direction.RowCol, "RC");
		HopsDirection2String.put(Direction.Col, "C");
		HopsDirection2String.put(Direction.Row, "R");
	}

	protected static final HashMap<Hop.AggOp, String> HopsAgg2String;
	static {
		HopsAgg2String = new HashMap<Hop.AggOp, String>();
		HopsAgg2String.put(AggOp.SUM, "+");
		HopsAgg2String.put(AggOp.SUM_SQ, "sq+");
		HopsAgg2String.put(AggOp.PROD, "*");
		HopsAgg2String.put(AggOp.MIN, "min");
		HopsAgg2String.put(AggOp.MAX, "max");
		HopsAgg2String.put(AggOp.MAXINDEX, "maxindex");
		HopsAgg2String.put(AggOp.MININDEX, "minindex");
		HopsAgg2String.put(AggOp.TRACE, "trace");
		HopsAgg2String.put(AggOp.MEAN, "mean");
		HopsAgg2String.put(AggOp.VAR, "var");
	}

	protected static final HashMap<Hop.ReOrgOp, String> HopsTransf2String;
	static {
		HopsTransf2String = new HashMap<ReOrgOp, String>();
		HopsTransf2String.put(ReOrgOp.TRANSPOSE, "t");
		HopsTransf2String.put(ReOrgOp.DIAG, "diag");
		HopsTransf2String.put(ReOrgOp.RESHAPE, "rshape");
		HopsTransf2String.put(ReOrgOp.SORT, "sort");
	}

	protected static final HashMap<DataOpTypes, String> HopsData2String;
	static {
		HopsData2String = new HashMap<Hop.DataOpTypes, String>();
		HopsData2String.put(DataOpTypes.PERSISTENTREAD, "PRead");
		HopsData2String.put(DataOpTypes.PERSISTENTWRITE, "PWrite");
		HopsData2String.put(DataOpTypes.TRANSIENTWRITE, "TWrite");
		HopsData2String.put(DataOpTypes.TRANSIENTREAD, "TRead");
	}
	
	public static boolean isFunction(OpOp2 op)
	{
		return op == OpOp2.MIN || op == OpOp2.MAX ||
		op == OpOp2.LOG;// || op == OpOp2.CONCAT; //concat is || in Netezza
	}
	
	public static boolean isSupported(OpOp2 op)
	{
		return op != OpOp2.INVALID && op != OpOp2.QUANTILE &&
		op != OpOp2.INTERQUANTILE && op != OpOp2.IQM;
	}
	
	public static boolean isFunction(OpOp1 op)
	{
		return op == OpOp1.SIN || op == OpOp1.TAN || op == OpOp1.COS ||
		op == OpOp1.ABS || op == OpOp1.EXP || op == OpOp1.LOG ||
		op == OpOp1.ROUND || op == OpOp1.SQRT;
	}
	
	public static boolean isBooleanOperation(OpOp2 op)
	{
		return op == OpOp2.AND || op == OpOp2.EQUAL ||
		op == OpOp2.GREATER || op == OpOp2.GREATEREQUAL ||
		op == OpOp2.LESS || op == OpOp2.LESSEQUAL ||
		op == OpOp2.OR;
	}
	
	/**
	 * 
	 * @param op
	 * @return
	 */
	public static OpOp2 getOpOp2ForOuterVectorOperation(String op) 
	{
		if( "+".equals(op) ) return OpOp2.PLUS;
		else if( "-".equals(op) ) return OpOp2.MINUS;
		else if( "*".equals(op) ) return OpOp2.MULT;
		else if( "/".equals(op) ) return OpOp2.DIV;
		else if( "%%".equals(op) ) return OpOp2.MODULUS;
		else if( "%/%".equals(op) ) return OpOp2.INTDIV;
		else if( "min".equals(op) ) return OpOp2.MIN;
		else if( "max".equals(op) ) return OpOp2.MAX;
		else if( "<=".equals(op) ) return OpOp2.LESSEQUAL;
		else if( "<".equals(op) ) return OpOp2.LESS;
		else if( ">=".equals(op) ) return OpOp2.GREATEREQUAL;
		else if( ">".equals(op) ) return OpOp2.GREATER;
		else if( "==".equals(op) ) return OpOp2.EQUAL;
		else if( "!=".equals(op) ) return OpOp2.NOTEQUAL;
		else if( "|".equals(op) ) return OpOp2.OR;
		else if( "&".equals(op) ) return OpOp2.AND;
		else if( "log".equals(op) ) return OpOp2.LOG;
		else if( "^".equals(op) ) return OpOp2.POW;
		
		return null;		
	}
	
	public static ValueType getResultValueType(ValueType vt1, ValueType vt2)
	{
		if(vt1 == ValueType.STRING || vt2  == ValueType.STRING)
			return ValueType.STRING;
		else if(vt1 == ValueType.DOUBLE || vt2 == ValueType.DOUBLE)
			return ValueType.DOUBLE;
		else
			return ValueType.INT;
	}
	
	/////////////////////////////////////
	// methods for dynamic re-compilation
	/////////////////////////////////////

	/**
	 * Indicates if dynamic recompilation is required for this hop. 
	 */
	public boolean requiresRecompile() 
	{
		return _requiresRecompile;
	}
	
	public void setRequiresRecompile()
	{
		_requiresRecompile = true;
	}
	
	public void unsetRequiresRecompile()
	{
		_requiresRecompile = false;
	}
	
	/**
	 * Update the output size information for this hop.
	 */
	public abstract void refreshSizeInformation();
	
	/**
	 * Util function for refreshing scalar rows input parameter.
	 */
	protected void refreshRowsParameterInformation( Hop input )
	{
		long size = computeSizeInformation(input);
			
		//always set the computed size not just if known (positive) in order to allow 
		//recompile with unknowns to reset sizes (otherwise potential for incorrect results)
		setDim1( size );
	}
	
	
	/**
	 * Util function for refreshing scalar cols input parameter.
	 */
	protected void refreshColsParameterInformation( Hop input )
	{
		long size = computeSizeInformation(input);
		
		//always set the computed size not just if known (positive) in order to allow 
		//recompile with unknowns to reset sizes (otherwise potential for incorrect results)
		setDim2( size );
	}
	
	/**
	 * 
	 * @param input
	 * @return
	 */
	public long computeSizeInformation( Hop input )
	{
		long ret = -1;
		
		try 
		{
			long tmp = OptimizerUtils.rEvalSimpleLongExpression(input, new HashMap<Long,Long>());
			if( tmp!=Long.MAX_VALUE )
				ret = tmp;
		}
		catch(Exception ex)
		{
			LOG.error("Failed to compute size information.", ex);
			ret = -1;
		}
		
		return ret;
	}
	
	/**
	 * 
	 * @param input
	 * @param vars
	 */
	public void refreshRowsParameterInformation( Hop input, LocalVariableMap vars )
	{
		long size = computeSizeInformation(input, vars);
		
		//always set the computed size not just if known (positive) in order to allow 
		//recompile with unknowns to reset sizes (otherwise potential for incorrect results)
		setDim1( size );
	}
	
	/**
	 * 
	 * @param input
	 * @param vars
	 */
	public void refreshColsParameterInformation( Hop input, LocalVariableMap vars )
	{
		long size = computeSizeInformation(input, vars);

		//always set the computed size not just if known (positive) in order to allow 
		//recompile with unknowns to reset sizes (otherwise potential for incorrect results)
		setDim2( size );
	}
	
	/**
	 * 
	 * @param input
	 * @param vars
	 * @return
	 */
	public long computeSizeInformation( Hop input, LocalVariableMap vars )
	{
		long ret = -1;
		
		try 
		{
			long tmp = OptimizerUtils.rEvalSimpleLongExpression(input, new HashMap<Long,Long>(), vars);
			if( tmp!=Long.MAX_VALUE )
				ret = tmp;
		}
		catch(Exception ex)
		{
			LOG.error("Failed to compute size information.", ex);
			ret = -1;
		}
		
		return ret;
	}
	
	/**
	 * 
	 * @param input
	 * @return
	 */
	public double computeBoundsInformation( Hop input ) 
	{
		double ret = Double.MAX_VALUE;
		
		try
		{
			ret = OptimizerUtils.rEvalSimpleDoubleExpression(input, new HashMap<Long, Double>());
		}
		catch(Exception ex)
		{
			LOG.error("Failed to compute bounds information.", ex);
			ret = Double.MAX_VALUE;
		}
		
		return ret;
	}
	
	/**
	 * Computes bound information for sequence if possible, otherwise returns
	 * Double.MAX_VALUE
	 * 
	 * @param input
	 * @param vars
	 * @return
	 */
	public double computeBoundsInformation( Hop input, LocalVariableMap vars ) 
	{
		double ret = Double.MAX_VALUE;
		
		try
		{
			ret = OptimizerUtils.rEvalSimpleDoubleExpression(input, new HashMap<Long, Double>(), vars);

		}
		catch(Exception ex)
		{
			LOG.error("Failed to compute bounds information.", ex);
			ret = Double.MAX_VALUE;
		}
		
		return ret;
	}
	
	
	/**
	 * Compute worst case estimate for size expression based on worst-case
	 * statistics of inputs. Limited set of supported operations in comparison
	 * to refresh rows/cols.
	 * 
	 * @param input
	 * @param memo
	 */
	protected long computeDimParameterInformation( Hop input, MemoTable memo )
	{
		long ret = -1;
		
		if( input instanceof UnaryOp )
		{
			if( ((UnaryOp)input).getOp() == Hop.OpOp1.NROW )
			{
				MatrixCharacteristics mc = memo.getAllInputStats(input.getInput().get(0));
				if( mc.getRows()>0 )
					ret = mc.getRows();
			}
			else if ( ((UnaryOp)input).getOp() == Hop.OpOp1.NCOL )
			{
				MatrixCharacteristics mc = memo.getAllInputStats(input.getInput().get(0));
				if( mc.getCols()>0 )
					ret = mc.getCols();
			}
		}
		else if ( input instanceof LiteralOp )
		{
			ret = UtilFunctions.parseToLong(input.getName());
		}
		else if ( input instanceof BinaryOp )
		{
			long dim = rEvalSimpleBinaryLongExpression(input, new HashMap<Long, Long>(), memo);
			if( dim != Long.MAX_VALUE ) //if known
				ret = dim ;
		}
		
		return ret;
	}
	
	/**
	 * 
	 * @param root
	 * @param valMemo
	 * @return
	 */
	protected long rEvalSimpleBinaryLongExpression( Hop root, HashMap<Long, Long> valMemo, MemoTable memo )
	{
		//memoization (prevent redundant computation of common subexpr)
		if( valMemo.containsKey(root.getHopID()) )
			return valMemo.get(root.getHopID());
		
		long ret = Long.MAX_VALUE;
		
		if( root instanceof LiteralOp )
		{
			long dim = UtilFunctions.parseToLong(root.getName());
			if( dim != -1 ) //if known
				ret = dim;
		}
		else if( root instanceof UnaryOp )
		{
			UnaryOp uroot = (UnaryOp) root;
			long dim = -1;
			if(uroot.getOp() == Hop.OpOp1.NROW)
			{
				MatrixCharacteristics mc = memo.getAllInputStats(uroot.getInput().get(0));
				dim = mc.getRows();
			}
			else if( uroot.getOp() == Hop.OpOp1.NCOL )
			{
				MatrixCharacteristics mc = memo.getAllInputStats(uroot.getInput().get(0));
				dim = mc.getCols();
			}
			if( dim != -1 ) //if known
				ret = dim;
		}
		else if( root instanceof BinaryOp )
		{ 
			if( OptimizerUtils.ALLOW_WORSTCASE_SIZE_EXPRESSION_EVALUATION )
			{
				BinaryOp broot = (BinaryOp) root;
				long lret = rEvalSimpleBinaryLongExpression(broot.getInput().get(0), valMemo, memo);
				long rret = rEvalSimpleBinaryLongExpression(broot.getInput().get(1), valMemo, memo);
				//note: positive and negative values might be valid subexpressions
				if( lret!=Long.MAX_VALUE && rret!=Long.MAX_VALUE ) //if known
				{
					switch( broot.getOp() )
					{
						case PLUS:	ret = lret + rret; break;
						case MULT:  ret = lret * rret; break;
						case MIN:   ret = Math.min(lret, rret); break;
						case MAX:   ret = Math.max(lret, rret); break;
						default:    ret = Long.MAX_VALUE;
					}
				}
				//exploit min constraint to propagate 
				else if( broot.getOp()==OpOp2.MIN && (lret!=Double.MAX_VALUE || rret!=Double.MAX_VALUE) )
				{
					ret = Math.min(lret, rret);
				}
			}
		}
		
		valMemo.put(root.getHopID(), ret);
		return ret;
	}


	/**
	 * 
	 * @return
	 */
	public String constructBaseDir()
	{
		StringBuilder sb = new StringBuilder();
		sb.append( ConfigurationManager.getScratchSpace() );
		sb.append( Lop.FILE_SEPARATOR );
		sb.append( Lop.PROCESS_PREFIX );
		sb.append( DMLScript.getUUID() );
		sb.append( Lop.FILE_SEPARATOR );
		sb.append( Lop.FILE_SEPARATOR );
		sb.append( ProgramConverter.CP_ROOT_THREAD_ID );
		sb.append( Lop.FILE_SEPARATOR );
	
		return sb.toString();
	}
	
	/**
	 * Clones the attributes of that and copies it over to this.
	 * 
	 * @param that
	 * @throws HopsException 
	 */
	protected void clone( Hop that, boolean withRefs ) 
		throws CloneNotSupportedException 
	{
		if( withRefs )
			throw new CloneNotSupportedException( "Hops deep copy w/ lops/inputs/parents not supported." );
		
		_ID = that._ID;
		_name = that._name;
		_dataType = that._dataType;
		_valueType = that._valueType;
		_visited = that._visited;
		_dim1 = that._dim1;
		_dim2 = that._dim2;
		_rows_in_block = that._rows_in_block;
		_cols_in_block = that._cols_in_block;
		_nnz = that._nnz;
		_updateInPlace = that._updateInPlace;

		//no copy of lops (regenerated)
		_parent = new ArrayList<Hop>();
		_input = new ArrayList<Hop>();
		_lops = null;
		
		_etype = that._etype;
		_etypeForced = that._etypeForced;
		_outputMemEstimate = that._outputMemEstimate;
		_memEstimate = that._memEstimate;
		_processingMemEstimate = that._processingMemEstimate;
		_requiresRecompile = that._requiresRecompile;
		_requiresReblock = that._requiresReblock;
		_requiresCheckpoint = that._requiresCheckpoint;
		_outputEmptyBlocks = that._outputEmptyBlocks;
		
		_beginLine = that._beginLine;
		_beginColumn = that._beginColumn;
		_endLine = that._endLine;
		_endColumn = that._endColumn;
	}

	public abstract Object clone() throws CloneNotSupportedException;
	
	public abstract boolean compare( Hop that );
	
	
	
	///////////////////////////////////////////////////////////////////////////
	// store position information for Hops
	///////////////////////////////////////////////////////////////////////////
	public int _beginLine, _beginColumn;
	public int _endLine, _endColumn;
	
	public void setBeginLine(int passed)    { _beginLine = passed;   }
	public void setBeginColumn(int passed) 	{ _beginColumn = passed; }
	public void setEndLine(int passed) 		{ _endLine = passed;   }
	public void setEndColumn(int passed)	{ _endColumn = passed; }
	
	public void setAllPositions(int blp, int bcp, int elp, int ecp){
		_beginLine	 = blp; 
		_beginColumn = bcp; 
		_endLine 	 = elp;
		_endColumn 	 = ecp;
	}

	public int getBeginLine()	{ return _beginLine;   }
	public int getBeginColumn() { return _beginColumn; }
	public int getEndLine() 	{ return _endLine;   }
	public int getEndColumn()	{ return _endColumn; }
	
	public String printErrorLocation(){
		return "ERROR: line " + _beginLine + ", column " + _beginColumn + " -- ";
	}
	
	public String printWarningLocation(){
		return "WARNING: line " + _beginLine + ", column " + _beginColumn + " -- ";
	}
	
	/**
	 * Sets the linenumbers of this hop to a given lop.
	 * 
	 * @param lop
	 */
	protected void setLineNumbers(Lop lop)
	{
		lop.setAllPositions(this.getBeginLine(), this.getBeginColumn(), this.getEndLine(), this.getEndColumn());
	}
	
} // end class
