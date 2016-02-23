package org.apache.sysml.runtime.controlprogram.context;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.controlprogram.Program;

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

    private synchronized static void initFlinkContext() {

    }
}
