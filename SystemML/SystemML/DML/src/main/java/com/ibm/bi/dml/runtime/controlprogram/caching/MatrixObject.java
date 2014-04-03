/**
 * IBM Confidential
 * OCO Source Materials
 * (C) Copyright IBM Corp. 2010, 2014
 * The source code for this program is not published or otherwise divested of its trade secrets, irrespective of what has been deposited with the U.S. Copyright Office.
 */

package com.ibm.bi.dml.runtime.controlprogram.caching;

import java.io.IOException;
import java.lang.ref.SoftReference;

import com.ibm.bi.dml.api.DMLScript;
import com.ibm.bi.dml.hops.OptimizerUtils;
import com.ibm.bi.dml.lops.Lop;
import com.ibm.bi.dml.parser.DMLTranslator;
import com.ibm.bi.dml.parser.Expression.DataType;
import com.ibm.bi.dml.parser.Expression.ValueType;
import com.ibm.bi.dml.runtime.DMLRuntimeException;
import com.ibm.bi.dml.runtime.controlprogram.ParForProgramBlock.PDataPartitionFormat;
import com.ibm.bi.dml.runtime.instructions.MRInstructions.RangeBasedReIndexInstruction.IndexRange;
import com.ibm.bi.dml.runtime.matrix.MatrixCharacteristics;
import com.ibm.bi.dml.runtime.matrix.MatrixDimensionsMetaData;
import com.ibm.bi.dml.runtime.matrix.MatrixFormatMetaData;
import com.ibm.bi.dml.runtime.matrix.MetaData;
import com.ibm.bi.dml.runtime.matrix.io.FileFormatProperties;
import com.ibm.bi.dml.runtime.matrix.io.InputInfo;
import com.ibm.bi.dml.runtime.matrix.io.MatrixBlock;
import com.ibm.bi.dml.runtime.matrix.io.NumItemsByEachReducerMetaData;
import com.ibm.bi.dml.runtime.matrix.io.OutputInfo;
import com.ibm.bi.dml.runtime.util.DataConverter;
import com.ibm.bi.dml.runtime.util.MapReduceTool;


/**
 * Represents a matrix in control program. This class contains method to read
 * matrices from HDFS and convert them to a specific format/representation. It
 * is also able to write several formats/representation of matrices to HDFS.

 * IMPORTANT: Preserve one-to-one correspondence between {@link MatrixObject}
 * and {@link MatrixBlock} objects, for cache purposes.  Do not change a
 * {@link MatrixBlock} object without informing its {@link MatrixObject} object.
 * 
 */
public class MatrixObject extends CacheableData
{
	@SuppressWarnings("unused")
	private static final String _COPYRIGHT = "Licensed Materials - Property of IBM\n(C) Copyright IBM Corp. 2010, 2014\n" +
                                             "US Government Users Restricted Rights - Use, duplication  disclosure restricted by GSA ADP Schedule Contract with IBM Corp.";
	
	/**
	 * Cache for actual data, evicted by garbage collector.
	 */
	private SoftReference<MatrixBlock> _cache = null;

	/**
	 * Container object that holds the actual data.
	 */
	private MatrixBlock _data = null;

	/**
	 * The name of HDFS file in which the data is backed up.
	 */
	private String _hdfsFileName = null; // file name and path
	
	/** 
	 * Flag that indicates whether or not hdfs file exists.
	 * It is used for improving the performance of "rmvar" instruction.
	 * When it has value <code>false</code>, one can skip invocations to
	 * utility functions such as MapReduceTool.deleteFileIfExistOnHDFS(),
	 * which can be potentially expensive.
	 */
	private boolean _hdfsFileExists = false; 
	
	/**
	 * <code>true</code> if the in-memory or evicted matrix may be different from
	 * the matrix located at {@link #_hdfsFileName}; <code>false</code> if the two
	 * matrices should be the same.
	 */
	private boolean _dirtyFlag = false;
	
	/**
	 * Object that holds the metadata associated with the matrix, which
	 * includes: 1) Matrix dimensions, if available 2) Number of non-zeros, if
	 * available 3) Block dimensions, if applicable 4) InputInfo -- subsequent
	 * operations that use this Matrix expect it to be in this format.
	 * 
	 * When the matrix is written to HDFS (local file system, as well?), one
	 * must get the OutputInfo that matches with InputInfo stored inside _mtd.
	 */
	private MetaData _metaData = null;
	
	//additional names and flags
	private String _varName = ""; //plan variable name
	private String _cacheFileName = null; //local eviction file name
	private boolean _requiresLocalWrite = false; //flag if local write for read obj
	private boolean _cleanupFlag = true; //flag if obj unpinned
	
	/**
	 * Information relevant to partitioned matrices.
	 */
	private boolean _partitioned = false; //indicates if obj partitioned
	private PDataPartitionFormat _partitionFormat = null; //indicates how obj partitioned
	private int _partitionSize = -1; //indicates n for BLOCKWISE_N
	private String _partitionCacheName = null; //name of cache block

	/**
	 * Informtaion relevant to specific external file formats
	 */
	FileFormatProperties _formatProperties = null;
	
	/**
	 * Constructor that takes only the HDFS filename.
	 */
	public MatrixObject (ValueType vt, String file)
	{
		this (vt, file, null); //HDFS file path
	}
	
	/**
	 * Constructor that takes both HDFS filename and associated metadata.
	 */
	public MatrixObject (ValueType vt, String file, MetaData mtd)
	{
		super (DataType.MATRIX, vt);
		_metaData = mtd; 
		_hdfsFileName = file;
		
		_cache = null;
		_data = null;
	}

	public void setVarName(String s) 
	{
		_varName = s;
	}
	
	public String getVarName() 
	{
		return _varName;
	}
	
	@Override
	public void setMetaData(MetaData md)
	{
		_metaData = md;
	}
	
	@Override
	public MetaData getMetaData()
	{
		return _metaData;
	}

	@Override
	public void removeMetaData()
	{
		_metaData = null;
	}
	
	@Override
	public void updateMatrixCharacteristics (MatrixCharacteristics mc)
	{
		((MatrixDimensionsMetaData)_metaData).setMatrixCharacteristics( mc );
	}

	/**
	 * Make the matrix metadata consistent with the in-memory matrix data
	 * @throws CacheException 
	 */
	public void refreshMetaData() 
		throws CacheException
	{
		if ( _data == null || _metaData ==null ) //refresh only for existing data
			throw new CacheException("Cannot refresh meta data because there is no data or meta data. "); 
		    //we need to throw an exception, otherwise input/output format cannot be inferred
		
		MatrixCharacteristics mc = ((MatrixDimensionsMetaData) _metaData).getMatrixCharacteristics();
		mc.setDimension( _data.getNumRows(),
						 _data.getNumColumns() );
		mc.setNonZeros( _data.getNonZeros() );		
	}

	public void setFileFormatProperties(FileFormatProperties formatProperties) {
		_formatProperties = formatProperties;
	}
	
	public FileFormatProperties getFileFormatProperties() {
		return _formatProperties;
	}
	
	public boolean isFileExists() 
	{
		return _hdfsFileExists;
	}
	
	public void setFileExists( boolean flag ) 
	{
		_hdfsFileExists = flag;
	}
	
	public String getFileName()
	{
		return _hdfsFileName;
	}

	public void setFileName( String file )
	{
		if (!_hdfsFileName.equals (file))
		{
			_hdfsFileName = file;
			if (! isEmpty ())
				_dirtyFlag = true;
		}
	}

	public long getNumRows () 
		throws DMLRuntimeException
	{
		if(_metaData == null)
			throw new DMLRuntimeException("No metadata available.");
		MatrixCharacteristics mc = ((MatrixDimensionsMetaData) _metaData).getMatrixCharacteristics();
		return mc.get_rows ();
	}

	public long getNumColumns() 
		throws DMLRuntimeException
	{
		if(_metaData == null)
			throw new DMLRuntimeException("No metadata available.");
		MatrixCharacteristics mc = ((MatrixDimensionsMetaData) _metaData).getMatrixCharacteristics();
		return mc.get_cols ();
	}

	public long getNnz() 
		throws DMLRuntimeException
	{
		if(_metaData == null)
			throw new DMLRuntimeException("No metadata available.");
		MatrixCharacteristics mc = ((MatrixDimensionsMetaData) _metaData).getMatrixCharacteristics();
		return mc.nonZero;
	}

	/**
	 * <code>true</code> if the in-memory or evicted matrix may be different from
	 * the matrix located at {@link #_hdfsFileName}; <code>false</code> if the two
	 * matrices are supposed to be the same.
	 */
	public boolean isDirty ()
	{
		return _dirtyFlag;
	}
	
	public String toString()
	{ 
		StringBuilder str = new StringBuilder();
		str.append("Matrix: ");
		str.append(_hdfsFileName + ", ");
		//System.out.println(_hdfsFileName);
		if ( _metaData instanceof NumItemsByEachReducerMetaData ) {
			str.append("NumItemsByEachReducerMetaData");
		} 
		else 
		{
			try
			{
				MatrixFormatMetaData md = (MatrixFormatMetaData)_metaData;
				if ( md != null ) {
					MatrixCharacteristics mc = ((MatrixDimensionsMetaData)_metaData).getMatrixCharacteristics();
					str.append(mc.toString());
					
					InputInfo ii = md.getInputInfo();
					if ( ii == null )
						str.append("null");
					else {
						if ( InputInfo.inputInfoToString(ii) == null ) {
							try {
								throw new DMLRuntimeException("Unexpected input format");
							} catch (DMLRuntimeException e) {
								e.printStackTrace();
							}
						}
						str.append(InputInfo.inputInfoToString(ii));
					}
				}
				else {
					str.append("null, null");
				}
			}
			catch(Exception ex)
			{
				LOG.error(ex);
			}
		}
		str.append(",");
		str.append(isDirty() ? "dirty" : "not-dirty");
		
		return str.toString();
	}
	
	
	// *********************************************
	// ***                                       ***
	// ***    HIGH-LEVEL METHODS THAT SPECIFY    ***
	// ***   THE LOCKING AND CACHING INTERFACE   ***
	// ***                                       ***
	// *********************************************
	
	
	/**
	 * Acquires a shared "read-only" lock, produces the reference to the matrix data,
	 * restores the matrix to main memory, reads from HDFS if needed.
	 * 
	 * Synchronized because there might be parallel threads (parfor local) that
	 * access the same MatrixObjectNew object (in case it was created before the loop).
	 * 
	 * In-Status:  EMPTY, EVICTABLE, EVICTED, READ;
	 * Out-Status: READ(+1).
	 * 
	 * @return the matrix data reference
	 * @throws CacheException 
	 */
	public synchronized MatrixBlock acquireRead()
		throws CacheException
	{
		LOG.trace("Acquire read "+_varName);
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		
		if ( !isAvailableToRead() )
			throw new CacheStatusException ("MatrixObject not available to read.");
		
		//get object from cache
		if( _data == null )
			getCache();
		
		//read data from HDFS if required
		if( isEmpty() && _data==null ) //probe data for jvm_reuse support  
		{
			//check filename
			if( _hdfsFileName == null )
				throw new CacheException("Cannot read matrix for empty filename.");
			
			try
			{
				if( DMLScript.STATISTICS )
					CacheStatistics.incrementHDFSHits();
				_data = readMatrixFromHDFS( _hdfsFileName );
				_dirtyFlag = false;
			}
			catch (IOException e)
			{
				throw new CacheIOException("Reading of " + _hdfsFileName + " ("+_varName+") failed.", e);
			}
			
			//mark for initial local write despite read operation
			_requiresLocalWrite = true;
		}
		else if( DMLScript.STATISTICS )
		{
			if( _data!=null )
				CacheStatistics.incrementMemHits();
		}
		acquire( false, _data==null );	
		
		if( DMLScript.STATISTICS ){
			long t1 = System.nanoTime();
			CacheStatistics.incrementAcquireRTime(t1-t0);
		}
		
		return _data;
	}
	
	/**
	 * Acquires the exclusive "write" lock for a thread that wants to change matrix
	 * cell values.  Produces the reference to the matrix data, restores the matrix
	 * to main memory, reads from HDFS if needed.
	 * 
	 * In-Status:  EMPTY, EVICTABLE, EVICTED;
	 * Out-Status: MODIFY.
	 * 
	 * @return the matrix data reference
	 * @throws CacheException 
	 */
	public synchronized MatrixBlock acquireModify() 
		throws CacheException
	{
		LOG.trace("Acquire modify "+_varName);
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		
		if ( !isAvailableToModify() )
			throw new CacheStatusException("MatrixObject not available to modify.");
		
		//get object from cache
		if( _data == null )
			getCache();
		
		//read data from HDFS if required
		if( isEmpty() )
		{
			//check filename
			if( _hdfsFileName == null )
				throw new CacheException("Cannot read matrix for empty filename.");
			
			//load data
			try
			{
				_data = readMatrixFromHDFS( _hdfsFileName );
			}
			catch (IOException e)
			{
				throw new CacheIOException("Reading of " + _hdfsFileName + " ("+_varName+") failed.", e);
			}
		}

		//cache status maintenance
		acquire( true, _data==null );
		_dirtyFlag = true;
		
		if( DMLScript.STATISTICS ){
			long t1 = System.nanoTime();
			CacheStatistics.incrementAcquireMTime(t1-t0);
		}
		
		return _data;
	}
	
	/**
	 * Acquires the exclusive "write" lock for a thread that wants to throw away the
	 * old matrix data and link up with new matrix data.  Abandons the old matrix data
	 * without reading it.  Sets the new matrix data reference.

	 * In-Status:  EMPTY, EVICTABLE, EVICTED;
	 * Out-Status: MODIFY.
	 * 
	 * @param newData : the new matrix data reference
	 * @return the matrix data reference, which is the same as the argument
	 * @throws CacheException 
	 */
	public synchronized MatrixBlock acquireModify(MatrixBlock newData)
		throws CacheException
	{
		LOG.trace("Acquire modify newdata "+_varName);
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		
		if (! isAvailableToModify ())
			throw new CacheStatusException ("MatrixObject not available to modify.");
		
		//clear old data 
		clearData(); 
		
		//cache status maintenance
		acquire (true, false); //no need to load evicted matrix
		_dirtyFlag = true;
		
		//set references to new data
		if (newData == null)
			throw new CacheException("acquireModify with empty matrix block.");
		_data = newData; 
		
		if( DMLScript.STATISTICS ){
			long t1 = System.nanoTime();
			CacheStatistics.incrementAcquireMTime(t1-t0);
		}
		
		return _data;
	}

	/**
	 * Releases the shared ("read-only") or exclusive ("write") lock.  Updates
	 * the matrix size, last-access time, metadata, etc.
	 * 
	 * Synchronized because there might be parallel threads (parfor local) that
	 * access the same MatrixObjectNew object (in case it was created before the loop).
	 * 
	 * In-Status:  READ, MODIFY;
	 * Out-Status: READ(-1), EVICTABLE, EMPTY.
	 * 
	 * @throws CacheStatusException
	 */
	@Override
	public synchronized void release() 
		throws CacheException
	{
		LOG.trace("Release "+_varName);
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		
		boolean write = false;
		if ( isModify() )
		{
			write = true;
			_dirtyFlag = true;
			refreshMetaData();
		}
		
		super.release();

		if(    isCachingActive() //only if caching is enabled (otherwise keep everything in mem) //TODO
			&& isCached() //not empty and not read/modify
		    && !isBelowCachingThreshold() ) //min size for caching
		{
			if( write || _requiresLocalWrite ) 
			{
				//evict blob
				String filePath = getCacheFilePathAndName();
				try
				{
					writeMatrix (filePath);
				}
				catch (Exception e)
				{
					throw new CacheException("Eviction to local path " + filePath + " ("+_varName+") failed.", e);
				}
				_requiresLocalWrite = false;
			}
			
			//create cache
			createCache();
			_data = null;			
		}
		else {
			LOG.trace("Var "+_varName+" not subject to caching: rows="+_data.getNumRows()+", cols="+_data.getNumColumns()+", state="+getStatusAsString());
		}
		
		if( DMLScript.STATISTICS ){
			long t1 = System.nanoTime();
			CacheStatistics.incrementReleaseTime(t1-t0);
		}
	}

	/**
	 * Sets the matrix data reference to <code>null</code>, abandons the old matrix.
	 * Makes the "envelope" empty.  Run it to finalize the matrix (otherwise the
	 * evicted matrix file may remain undeleted).
	 * 
	 * In-Status:  EMPTY, EVICTABLE, EVICTED;
	 * Out-Status: EMPTY.
	 * @throws CacheException 
	 */
	public synchronized void clearData() 
		throws CacheException
	{
		LOG.trace("Clear data "+_varName);
		
		// check if cleanup enabled and possible 
		if( !_cleanupFlag ) 
			return; // do nothing
		if( !isAvailableToModify() )
			throw new CacheStatusException ("MatrixObject (" + this.getDebugName() + ") not available to modify. Status = " + this.getStatusAsString() + ".");
		
		// clear existing WB / FS representation (but prevent unnecessary probes)
		if( !(isEmpty()||(_data!=null && isBelowCachingThreshold()) ))
			freeEvictedBlob();	
		
		// clear the in-memory data
		_data = null;	
		clearCache();
		
		// change object state EMPTY
		_dirtyFlag = false;
		setEmpty();
	}
	
	public void exportData()
		throws CacheException
	{
		exportData( -1 );
	}
	
	/**
	 * Writes, or flushes, the matrix data to HDFS.
	 * 
	 * In-Status:  EMPTY, EVICTABLE, EVICTED, READ;
	 * Out-Status: EMPTY, EVICTABLE, EVICTED, READ.
	 * 
	 * @throws CacheException 
	 */
	public void exportData( int replication )
		throws CacheException
	{
		exportData(_hdfsFileName, null, replication, null);
		_hdfsFileExists = true;
	}
	
	/**
	 * 
	 * @param fName
	 * @param outputFormat
	 * @param formatProperties
	 * @throws CacheException
	 */
	public synchronized void exportData (String fName, String outputFormat, FileFormatProperties formatProperties)
		throws CacheException
	{
		exportData(fName, outputFormat, -1, formatProperties);
	}
	
	/**
	 * 
	 * @param fName
	 * @param outputFormat
	 * @throws CacheException
	 */
	public synchronized void exportData (String fName, String outputFormat)
		throws CacheException
	{
		exportData(fName, outputFormat, -1, null);
	}
	
	/**
	 * Synchronized because there might be parallel threads (parfor local) that
	 * access the same MatrixObjectNew object (in case it was created before the loop).
	 * If all threads export the same data object concurrently it results in errors
	 * because they all write to the same file. Efficiency for loops and parallel threads
	 * is achieved by checking if the in-memory matrix block is dirty.
	 * 
	 * NOTE: MB: we do not use dfs copy from local (evicted) to HDFS because this would ignore
	 * the output format and most importantly would bypass reblocking during write (which effects the
	 * potential degree of parallelism). However, we copy files on HDFS if certain criteria are given.  
	 * 
	 * @param fName
	 * @param outputFormat
	 * @throws CacheException
	 */
	public synchronized void exportData (String fName, String outputFormat, int replication, FileFormatProperties formatProperties)
		throws CacheException
	{
		LOG.trace("Export data "+_varName+" "+fName);
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		
		//prevent concurrent modifications
		if ( !isAvailableToRead() )
			throw new CacheStatusException ("MatrixObject not available to read.");

		LOG.trace("Exporting " + this.getDebugName() + " to " + fName + " in format " + outputFormat);
				
		boolean pWrite = false; // !fName.equals(_hdfsFileName); //persistent write flag
		if ( fName.equals(_hdfsFileName) ) {
			_hdfsFileExists = true;
			pWrite = false;
		}
		else {
			pWrite = true;  // i.e., export is called from "write" instruction
		}

		  //actual export (note: no direct transfer of local copy in order to ensure blocking (and hence, parallelism))
		  if(  isDirty()  ||      //use dirty for skipping parallel exports
		    (pWrite && !isEqualOutputFormat(outputFormat)) ) 
		  {
		  
			// CASE 1: dirty in-mem matrix or pWrite w/ different format (write matrix to fname; load into memory if evicted)
			// a) get the matrix		
			if( isEmpty() )
			{
			    //read data from HDFS if required (never read before), this applies only to pWrite w/ different output formats
				try
				{
			        _data = readMatrixFromHDFS( _hdfsFileName );
					_dirtyFlag = false;
				}
				catch (IOException e)
				{
				    throw new CacheIOException("Reading of " + _hdfsFileName + " ("+_varName+") failed.", e);
				}
			}
			//get object from cache
			if( _data == null )
				getCache();
			acquire( false, _data==null ); //incl. read matrix if evicted	
			
			// b) write the matrix 
			try
			{
				writeMetaData( fName, outputFormat );
				writeMatrixToHDFS( fName, outputFormat, replication, formatProperties );
				if ( !pWrite )
					_dirtyFlag = false;
			}
			catch (Exception e)
			{
				throw new CacheIOException ("Export to " + fName + " failed.", e);
			}
			finally
			{
				release();
			}
		}
		else if( pWrite ) // pwrite with same output format
		{
			//CASE 2: matrix already in same format but different file on hdfs (copy matrix to fname)
			try
			{
				MapReduceTool.deleteFileIfExistOnHDFS(fName);
				MapReduceTool.deleteFileIfExistOnHDFS(fName+".mtd");
				writeMetaData( fName, outputFormat );
				MapReduceTool.copyFileOnHDFS( _hdfsFileName, fName );
			}
			catch (Exception e)
			{
				throw new CacheIOException ("Export to " + fName + " failed.", e);
			}
		}
		else 
		{
			//CASE 3: data already in hdfs (do nothing, no need for export)
			LOG.trace(this.getDebugName() + ": Skip export to hdfs since data already exists.");
		}
		  
		if( DMLScript.STATISTICS ){
			long t1 = System.nanoTime();
			CacheStatistics.incrementExportTime(t1-t0);
		}
	}

	
	// *********************************************
	// ***                                       ***
	// ***       HIGH-LEVEL PUBLIC METHODS       ***
	// ***     FOR PARTITIONED MATRIX ACCESS     ***
	// ***   (all other methods still usable)    ***
	// ***                                       ***
	// *********************************************
	
	/**
	 * @param n 
	 * 
	 */
	public void setPartitioned( PDataPartitionFormat format, int n )
	{
		_partitioned = true;
		_partitionFormat = format;
		_partitionSize = n;
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean isPartitioned()
	{
		return _partitioned;
	}
	
	public PDataPartitionFormat getPartitionFormat()
	{
		return _partitionFormat;
	}
	
	public int getPartitionSize()
	{
		return _partitionSize;
	}
	
	/**
	 * NOTE: for reading matrix partitions, we could cache (in its real sense) the read block
	 * with soft references (no need for eviction, as partitioning only applied for read-only matrices).
	 * However, since we currently only support row- and column-wise partitioning caching is not applied yet.
	 * This could be changed once we also support column-block-wise and row-block-wise. Furthermore,
	 * as we reject to partition vectors and support only full row or column indexing, no metadata (apart from
	 * the partition flag) is required.  
	 * 
	 * @param pred
	 * @return
	 * @throws CacheException
	 */
	public synchronized MatrixBlock readMatrixPartition( IndexRange pred ) 
		throws CacheException
	{
		LOG.trace("Acquire partition "+_varName+" "+pred);
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		
		if ( !_partitioned )
			throw new CacheStatusException ("MatrixObject not available to indexed read.");
		
		MatrixBlock mb = null;
		
		try
		{
			boolean blockwise = (_partitionFormat==PDataPartitionFormat.ROW_BLOCK_WISE || _partitionFormat==PDataPartitionFormat.COLUMN_BLOCK_WISE);
			
			//preparations for block wise access
			MatrixFormatMetaData iimd = (MatrixFormatMetaData) _metaData;
			MatrixCharacteristics mc = iimd.getMatrixCharacteristics();
			int brlen = mc.get_rows_per_block();
			int bclen = mc.get_cols_per_block();
			
			//get filename depending on format
			String fname = getPartitionFileName( pred, brlen, bclen );
			
			//probe cache
			if( blockwise && _partitionCacheName != null && _partitionCacheName.equals(fname) )
			{
				mb = _cache.get(); //try getting block from cache
			}
			
			if( mb == null ) //block not in cache
			{
				//get rows and cols
				long rows = -1;
				long cols = -1;
				switch( _partitionFormat )
				{
					case ROW_WISE:
						rows = 1;
						cols = mc.get_cols();
						break;
					case ROW_BLOCK_WISE: 
						rows = brlen;
						cols = mc.get_cols();
						break;
					case COLUMN_WISE:
						rows = mc.get_rows();
						cols = 1;
						break;
					case COLUMN_BLOCK_WISE: 
						rows = mc.get_rows();
						cols = bclen;
						break;
				}
				
				
				//read the 
				if( MapReduceTool.existsFileOnHDFS(fname) )
					mb = readMatrixFromHDFS( fname, rows, cols );
				else
				{
					mb = new MatrixBlock((int)rows, (int)cols, true);
					LOG.warn("Reading empty matrix partition "+fname);
				}
			}
			
			//post processing
			if( blockwise )
			{
				//put block into cache
				_partitionCacheName = fname;
				_cache = new SoftReference<MatrixBlock>(mb);
				
				if( _partitionFormat == PDataPartitionFormat.ROW_BLOCK_WISE )
				{
					long rix = (pred.rowStart-1)%brlen+1;
					mb = (MatrixBlock) mb.sliceOperations(rix, rix, pred.colStart, pred.colEnd, new MatrixBlock());
				}
				if( _partitionFormat == PDataPartitionFormat.COLUMN_BLOCK_WISE )
				{
					long cix = (pred.colStart-1)%bclen+1;
					mb = (MatrixBlock) mb.sliceOperations(pred.rowStart, pred.rowEnd, cix, cix, new MatrixBlock());
				}
			}
			
			//NOTE: currently no special treatment of non-existing partitions necessary 
			//      because empty blocks are written anyway
		}
		catch(Exception ex)
		{
			throw new CacheException(ex);
		}
		
		if( DMLScript.STATISTICS ){
			long t1 = System.nanoTime();
			CacheStatistics.incrementAcquireRTime(t1-t0);
		}
		
		return mb;
	}
	
	
	/**
	 * 
	 * @param pred
	 * @return
	 * @throws CacheStatusException 
	 */
	public String getPartitionFileName( IndexRange pred, int brlen, int bclen ) 
		throws CacheStatusException
	{
		if ( !_partitioned )
			throw new CacheStatusException ("MatrixObject not available to indexed read.");
		
		StringBuilder sb = new StringBuilder();
		sb.append(_hdfsFileName);
		
		switch( _partitionFormat )
		{
			case ROW_WISE:
				sb.append(Lop.FILE_SEPARATOR);
				sb.append(pred.rowStart); 
				break;
			case ROW_BLOCK_WISE:
				sb.append(Lop.FILE_SEPARATOR);
				sb.append((pred.rowStart-1)/brlen+1);
				break;
			case COLUMN_WISE:
				sb.append(Lop.FILE_SEPARATOR);
				sb.append(pred.colStart);
				break;
			case COLUMN_BLOCK_WISE:
				sb.append(Lop.FILE_SEPARATOR);
				sb.append((pred.colStart-1)/bclen+1);
				break;
			default:
				throw new CacheStatusException ("MatrixObject not available to indexed read.");
		}

		return sb.toString();
	}	
	
	

	// *********************************************
	// ***                                       ***
	// ***      LOW-LEVEL PROTECTED METHODS      ***
	// ***         EXTEND CACHEABLE DATA         ***
	// ***     ONLY CALLED BY THE SUPERCLASS     ***
	// ***                                       ***
	// *********************************************
	

	@Override
	public boolean isBlobPresent()
	{
		return (_data != null);
	}

	@Override
	protected void evictBlobFromMemory ( MatrixBlock mb ) 
		throws CacheIOException
	{
		throw new CacheIOException("Redundant explicit eviction.");
	}
	
	@Override
	protected void restoreBlobIntoMemory () 
		throws CacheIOException
	{
		LOG.trace("RESTORE of Matrix "+_varName+", "+_hdfsFileName);
		
		String filePath = getCacheFilePathAndName();
		long begin = System.currentTimeMillis();
		LOG.trace ("CACHE: Restoring matrix...  " + _varName + "  HDFS path: " + 
					(_hdfsFileName == null ? "null" : _hdfsFileName) + ", Restore from path: " + filePath);
				
		if (_data != null)
			throw new CacheIOException (filePath + " : Cannot restore on top of existing in-memory data.");

		try
		{
			_data = readMatrix(filePath);
		}
		catch (IOException e)
		{
			throw new CacheIOException (filePath + " : Restore failed.", e);	
		}
		
		//check for success
	    if (_data == null)
			throw new CacheIOException (filePath + " : Restore failed.");
	    
	    LOG.trace("Restoring matrix - COMPLETED ... " + (System.currentTimeMillis()-begin) + " msec.");
	}		

	@Override
	protected void freeEvictedBlob()
	{
		String cacheFilePathAndName = getCacheFilePathAndName();
		long begin = System.currentTimeMillis();
		LOG.trace("CACHE: Freeing evicted matrix...  " + _varName + "  HDFS path: " + 
					(_hdfsFileName == null ? "null" : _hdfsFileName) + " Eviction path: " + cacheFilePathAndName);
				
		switch (CacheableData.cacheEvictionStorageType)
		{
			case LOCAL:
				LazyWriteBuffer.deleteMatrix(cacheFilePathAndName);
				//File f = new File(cacheFilePathAndName);
				//if( f.exists() )
				//	f.delete();
				break;
			case HDFS:
				try{
					MapReduceTool.deleteFileIfExistOnHDFS(cacheFilePathAndName); 
				}
				catch (IOException e){}
				break;
		}
		
		LOG.trace("Freeing evicted matrix - COMPLETED ... " + (System.currentTimeMillis()-begin) + " msec.");
		
	}
	
	@Override
	protected boolean isBelowCachingThreshold()
	{
		long rlen = _data.getNumRows();
		long clen = _data.getNumColumns();
		long nnz = _data.getNonZeros();
		
		//get in-memory size (assume dense, if nnz unknown)
		double sparsity = OptimizerUtils.getSparsity( rlen, clen, nnz );
		double size = MatrixBlock.estimateSize( rlen, clen, sparsity ); 
		
		return ( size <= CACHING_THRESHOLD );
	}
	
	// *******************************************
	// ***                                     ***
	// ***      LOW-LEVEL PRIVATE METHODS      ***
	// ***           FOR MATRIX I/O            ***
	// ***                                     ***
	// *******************************************
	
	/**
	 * 
	 */
	private String getCacheFilePathAndName ()
	{
		if( _cacheFileName==null )
		{
			StringBuilder sb = new StringBuilder();
			switch (CacheableData.cacheEvictionStorageType)
			{
				case LOCAL:
					sb.append(CacheableData.cacheEvictionLocalFilePath); 
					sb.append(CacheableData.cacheEvictionLocalFilePrefix);
					sb.append(String.format ("%09d", getUniqueCacheID()));
					sb.append(CacheableData.cacheEvictionLocalFileExtension);
					break;
					
				case HDFS:
					sb.append(CacheableData.cacheEvictionHDFSFilePath);
					sb.append(CacheableData.cacheEvictionHDFSFilePrefix);
					sb.append(String.format ("%09d", getUniqueCacheID()));
					sb.append(CacheableData.cacheEvictionHDFSFileExtension);
					break;
			}
			
			_cacheFileName = sb.toString();
		}
		
		return _cacheFileName;
	}
	
	/**
	 * 
	 * @param filePathAndName
	 * @return
	 * @throws IOException
	 */
	private MatrixBlock readMatrix (String filePathAndName)
		throws IOException
	{
		//System.out.println("read matrix "+filePathAndName);
		
		MatrixBlock newData = null;
		switch (CacheableData.cacheEvictionStorageType)
		{
			case LOCAL:
				newData = LazyWriteBuffer.readMatrix(filePathAndName);
				//newData = LocalFileUtils.readMatrixBlockFromLocal(filePathAndName);
				break;			
			case HDFS:
				newData = readMatrixFromHDFS (filePathAndName);
				break;
			default:
				throw new IOException (filePathAndName + 
						" : Cannot read matrix from unsupported storage type \""
						+ CacheableData.cacheEvictionStorageType + "\"");				
		}
		return newData;
	}
	
	/**
	 * 
	 * @param filePathAndName
	 * @return
	 * @throws IOException
	 */
	private MatrixBlock readMatrixFromHDFS(String filePathAndName)
		throws IOException
	{
		MatrixFormatMetaData iimd = (MatrixFormatMetaData) _metaData;
		MatrixCharacteristics mc = iimd.getMatrixCharacteristics();
		return readMatrixFromHDFS( filePathAndName, mc.get_rows(), mc.get_cols() );
	}
	
	/**
	 * 
	 * @param filePathAndName
	 * @param rlen
	 * @param clen
	 * @return
	 * @throws IOException
	 */
	private MatrixBlock readMatrixFromHDFS(String filePathAndName, long rlen, long clen)
		throws IOException
	{

		long begin = System.currentTimeMillis();;
		
		MatrixFormatMetaData iimd = (MatrixFormatMetaData) _metaData;
		MatrixCharacteristics mc = iimd.getMatrixCharacteristics();
		
		LOG.trace("Reading matrix from HDFS...  " + _varName + "  Path: " + filePathAndName 
				+ ", dimensions: [" + mc.numRows + ", " + mc.numColumns + ", " + mc.nonZero + "]");
			
		double sparsity = ( mc.nonZero >= 0 ? ((double)mc.nonZero)/(mc.numRows*mc.numColumns) : 1.0d) ; //expected sparsity
		MatrixBlock newData = DataConverter.readMatrixFromHDFS(filePathAndName, iimd.getInputInfo(),
				                           rlen, clen, mc.numRowsPerBlock, mc.numColumnsPerBlock, sparsity, _formatProperties);
		
		if( newData == null )
		{
			throw new IOException("Unable to load matrix from file "+filePathAndName);
		}
		
		//System.out.println("Reading Completed: " + (System.currentTimeMillis()-begin) + " msec.");
		LOG.trace("Reading Completed: " + (System.currentTimeMillis()-begin) + " msec.");
		return newData;
	}
	
	/**
	 * 
	 * @param filePathAndName
	 * @throws DMLRuntimeException
	 * @throws IOException
	 */
	private void writeMatrix (String filePathAndName)
		throws DMLRuntimeException, IOException
	{
		//System.out.println("write matrix "+filePathAndName );
		
		switch (CacheableData.cacheEvictionStorageType)
		{
			case LOCAL:
				LazyWriteBuffer.writeMatrix(filePathAndName, _data);
				//LocalFileUtils.writeMatrixBlockToLocal(filePathAndName, _data);
				
				//TODO just NIO read/write, but our test is still experimental
				//if( _data.isInSparseFormat() || _data.getDenseArray()==null )
				//	LocalFileUtils.writeMatrixBlockToLocal(filePathAndName, _data);
				//else
				//	LocalFileUtils.writeMatrixBlockToLocal2(filePathAndName, _data);
				break;			
			case HDFS:
				writeMatrixToHDFS (filePathAndName, null, -1, null);
				break;
			default:
				throw new IOException (filePathAndName + 
						" : Cannot write matrix to unsupported storage type \""
						+ CacheableData.cacheEvictionStorageType + "\"");				
		}
		
	}

	/**
	 * Writes in-memory matrix to HDFS in a specified format.
	 * 
	 * @throws DMLRuntimeException
	 * @throws IOException
	 */
	private void writeMatrixToHDFS (String filePathAndName, String outputFormat, int replication, FileFormatProperties formatProperties)
		throws DMLRuntimeException, IOException
	{
		//System.out.println("write matrix "+_varName+" "+filePathAndName);
		
		long begin = System.currentTimeMillis();
		LOG.trace (" Writing matrix to HDFS...  " + _varName + "  Path: " + filePathAndName + ", Format: " +
					(outputFormat != null ? outputFormat : "inferred from metadata"));
		
		MatrixFormatMetaData iimd = (MatrixFormatMetaData) _metaData;

		if (_data != null)
		{
			// Get the dimension information from the metadata stored within MatrixObject
			MatrixCharacteristics mc = iimd.getMatrixCharacteristics ();
			// Write the matrix to HDFS in requested format
			OutputInfo oinfo = (outputFormat != null ? OutputInfo.stringToOutputInfo (outputFormat) 
					                                 : InputInfo.getMatchingOutputInfo (iimd.getInputInfo ()));
			
			// when outputFormat is binaryblock, make sure that matrixCharacteristics has correct blocking dimensions
			if ( oinfo == OutputInfo.BinaryBlockOutputInfo && 
					(mc.get_rows_per_block() != DMLTranslator.DMLBlockSize || mc.get_cols_per_block() != DMLTranslator.DMLBlockSize) ) {
				DataConverter.writeMatrixToHDFS(_data, filePathAndName, oinfo, new MatrixCharacteristics(mc.get_rows(), mc.get_cols(), DMLTranslator.DMLBlockSize, DMLTranslator.DMLBlockSize, mc.getNonZeros()), replication, formatProperties);
			}
			else {
				DataConverter.writeMatrixToHDFS(_data, filePathAndName, oinfo, mc, replication, formatProperties);
			}

			LOG.trace("Writing matrix to HDFS ("+filePathAndName+") - COMPLETED... " + (System.currentTimeMillis()-begin) + " msec.");
		}
		else 
		{
			LOG.trace ("Writing matrix to HDFS ("+filePathAndName+") - NOTHING TO WRITE (_data == null).");
		}
		
		if( DMLScript.STATISTICS )
			CacheStatistics.incrementHDFSWrites();
	}
	
	/**
	 * 
	 * @param filePathAndName
	 * @param outputFormat
	 * @throws DMLRuntimeException
	 * @throws IOException
	 */
	private void writeMetaData (String filePathAndName, String outputFormat)
		throws DMLRuntimeException, IOException
	{
		MatrixFormatMetaData iimd = (MatrixFormatMetaData) _metaData;
	
		if (iimd != null)
		{
			// Write the matrix to HDFS in requested format			
			OutputInfo oinfo = (outputFormat != null ? OutputInfo.stringToOutputInfo (outputFormat) 
                    : InputInfo.getMatchingOutputInfo (iimd.getInputInfo ()));
			
			if ( oinfo != OutputInfo.MatrixMarketOutputInfo ) {
				// Get the dimension information from the metadata stored within MatrixObject
				MatrixCharacteristics mc = iimd.getMatrixCharacteristics ();
				
				// when outputFormat is binaryblock, make sure that matrixCharacteristics has correct blocking dimensions
				if ( oinfo == OutputInfo.BinaryBlockOutputInfo && 
						(mc.get_rows_per_block() != DMLTranslator.DMLBlockSize || mc.get_cols_per_block() != DMLTranslator.DMLBlockSize) ) {
					mc = new MatrixCharacteristics(mc.get_rows(), mc.get_cols(), DMLTranslator.DMLBlockSize, DMLTranslator.DMLBlockSize, mc.getNonZeros());
				}
				MapReduceTool.writeMetaDataFile (filePathAndName + ".mtd", valueType, mc, oinfo);
			}
		}
		else {
			throw new DMLRuntimeException("Unexpected error while writing mtd file (" + filePathAndName + ") -- metadata is null.");
		}
	}
	
	/**
	 * 
	 * @param outputFormat
	 * @return
	 */
	private boolean isEqualOutputFormat( String outputFormat )
	{
		boolean ret = true;
		
		if( outputFormat != null )
		{
			try
			{
				MatrixFormatMetaData iimd = (MatrixFormatMetaData) _metaData;
				OutputInfo oi1 = InputInfo.getMatchingOutputInfo( iimd.getInputInfo() );
				OutputInfo oi2 = OutputInfo.stringToOutputInfo( outputFormat );
				if( oi1 != oi2 )
				{
					ret = false;
				}
			}
			catch(Exception ex)
			{
				ret = false;
			}
		}
		
		return ret;
	}
	
	@Override
	public synchronized String getDebugName()
	{
		int maxLength = 23;
		String debugNameEnding = (_hdfsFileName == null ? "null" : 
			(_hdfsFileName.length() < maxLength ? _hdfsFileName : "..." + 
				_hdfsFileName.substring (_hdfsFileName.length() - maxLength + 3)));
		return _varName + " " + debugNameEnding;
	}

	
	// *******************************************
	// ***                                     ***
	// ***      LOW-LEVEL PRIVATE METHODS      ***
	// ***       FOR SOFTREFERENCE CACHE       ***
	// ***                                     ***
	// *******************************************
	
	/**
	 * 
	 */
	private void createCache( ) 
	{
		_cache = new SoftReference<MatrixBlock>( _data );	
	}

	/**
	 * 
	 */
	private void getCache()
	{
		if( _cache !=null )
		{
			_data = _cache.get();
			clearCache();
		}
	}

	/**
	 * 
	 */
	private void clearCache()
	{
		if( _cache != null )
		{
			_cache.clear();
			_cache = null;
		}
	}
	
	/**
	 * see clear data
	 * 
	 * @param flag
	 */
	public void enableCleanup(boolean flag) 
	{
		_cleanupFlag = flag;
	}

	/**
	 * see clear data
	 * 
	 * @return
	 */
	public boolean isCleanupEnabled() 
	{
		return _cleanupFlag;
	}

	/**
	 * 
	 */
	public void setEmptyStatus()
	{
		setEmpty();
	}
}