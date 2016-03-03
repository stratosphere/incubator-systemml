package org.apache.sysml.runtime.instructions.flink;

import org.apache.sysml.api.DMLScript;
import org.junit.Test;

public class FlinkExecutionTest {

    @Test
    public void executeDMLScript() throws Exception {
        String[] args = {"-f",
                "/home/fschueler/Repos/incubator-systemml/scripts/myScripts/tsmm.dml",
                "-config=/home/fschueler/Repos/incubator-systemml/conf/SystemML-config.xml",
                "-exec",
                "hybrid_flink"};
        DMLScript.main(args);
    }
}
