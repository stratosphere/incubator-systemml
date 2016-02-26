package org.apache.sysml.runtime.controlprogram.context;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.Program;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.instructions.flink.data.DataSetObject;
import org.apache.sysml.runtime.matrix.data.InputInfo;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

import java.io.IOException;

public class FlinkExecutionContext extends ExecutionContext {

    private static final Log LOG = LogFactory.getLog(FlinkExecutionContext.class.getName());

    private static ExecutionEnvironment _execEnv = null;

    protected FlinkExecutionContext(Program prog) {
        this(true, prog);
    }

    protected FlinkExecutionContext(boolean allocateVars, Program prog) {
        super(allocateVars, prog);

        if (OptimizerUtils.isHybridExecutionMode())
            initFlinkContext();
    }

    public ExecutionEnvironment getFlinkContext() {
        return _execEnv;
    }

    public DataSet<Tuple2<MatrixIndexes, MatrixBlock>> getBinaryBlockDataSetHandleForVariable(String varname)
        throws DMLRuntimeException, DMLUnsupportedOperationException {

        return (DataSet<Tuple2<MatrixIndexes, MatrixBlock>>) getDataSetHandleForVariable(varname, InputInfo.BinaryBlockInputInfo);
    }

    public DataSet<?> getDataSetHandleForVariable(String varname, InputInfo inputInfo)
        throws DMLRuntimeException, DMLUnsupportedOperationException {

        MatrixObject mo = getMatrixObject(varname);
        return getDataSetHandleForMatrixObject(mo, inputInfo);
    }

    public void setDataSetHandleForVariable(String varname, DataSet<Tuple2<MatrixIndexes, MatrixBlock>> ds) throws DMLRuntimeException {
        MatrixObject mo = getMatrixObject(varname);
        DataSetObject dsHandle = new DataSetObject(ds, varname);
        mo.setDataSetHandle(dsHandle);
    }

    public void addLineageDataSet(String varParent, String varChild) throws DMLRuntimeException {
        DataSetObject parent = getMatrixObject(varParent).getDataSetHandle();
        DataSetObject child  = getMatrixObject(varChild).getDataSetHandle();

        parent.addLineageChild(child);
    }

    private DataSet<?> getDataSetHandleForMatrixObject(MatrixObject mo, InputInfo inputInfo)
        throws DMLRuntimeException, DMLUnsupportedOperationException {

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
            //TODO
            mo.getDebugName();
        }
        //CASE 3: non-dirty (file exists on HDFS)
        else {
            if (inputInfo == InputInfo.BinaryBlockInputInfo) {
                //TODO
            } else if (inputInfo == InputInfo.TextCellInputInfo || inputInfo == InputInfo.CSVInputInfo || inputInfo == InputInfo.MatrixMarketInputInfo) {
                // Set up the Hadoop TextInputFormat.
                HadoopInputFormat<LongWritable, Text> hadoopIF;
                try {
                    Job job = Job.getInstance();
                    hadoopIF =
                            new HadoopInputFormat<LongWritable, Text>(
                                    new TextInputFormat(), LongWritable.class, Text.class, job
                            );
                    TextInputFormat.addInputPath(job, new Path(mo.getFileName()));
                } catch (IOException ioe) {
                    throw new DMLRuntimeException("Could not read resource from hdfs: " + mo.getFileName());
                }

                // Read data using the Hadoop TextInputFormat.
                dataSet = getFlinkContext().createInput(hadoopIF);
                //FIXME (this fails with nullpointer exception)
            } else if(inputInfo == InputInfo.BinaryCellInputInfo) {
                //TODO
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

    }
}
