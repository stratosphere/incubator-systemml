package org.apache.sysml.runtime.controlprogram.context;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.Program;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.matrix.data.InputInfo;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.udf.Matrix;

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

    private DataSet<?> getDataSetHandleForVariable(String varname, InputInfo inputInfo)
        throws DMLRuntimeException, DMLUnsupportedOperationException {

        MatrixObject mo = getMatrixObject(varname);
        return getDataSetHandleForMatrixObject(mo, inputInfo);
    }

    private DataSet<?> getDataSetHandleForMatrixObject(MatrixObject mo, InputInfo inputInfo)
        throws DMLRuntimeException, DMLUnsupportedOperationException {

        //FIXME this logic should be in matrix-object (see spark version of this method for more info)
        DataSet<?> dataSet = null;

        //CASE 1: rdd already existing (reuse if checkpoint or trigger
        //pending rdd operations if not yet cached but prevent to re-evaluate
        //rdd operations if already executed and cached
        if(    mo.getDataSetHandle()!=null
                && (mo.getDataSetHandle().isCheckpointed() || !mo.isCached(false)) )
        {
            //return existing rdd handling (w/o input format change)
            dataSet = mo.getDataSetHandle().getDataSet();
        }
        //FIXME omitting further cases here
        return dataSet;
    }

    private synchronized static void initFlinkContext() {

    }
}
