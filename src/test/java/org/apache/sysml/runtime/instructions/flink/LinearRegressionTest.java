package org.apache.sysml.runtime.instructions.flink;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.sysml.api.DMLScript;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class LinearRegressionTest {
    final static String scriptsPath = "/data/repos/incubator-systemml/scripts";
    static File   tempDir;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        String script = scriptsPath + "/datagen/genLinearRegressionData.dml";
        tempDir       = Files.createTempDir();
        String tmp    = tempDir.getCanonicalPath();

        String[] args = {
                "-f",
                script,
                "-nvargs",
                "numSamples=1000",
                "numFeatures=50",
                "maxFeatureValue=5",
                "maxWeight=5",
                "addNoise=FALSE",
                "b=0",
                "sparsity=0.7",
                "output=" + tmp + "/linRegData.csv",
                "format=csv",
                "perc=0.5"
        };
        DMLScript.main(args);

//        This generates the following files inside the ./temp folder:
//
//        linRegData.csv      # 1000 rows of 51 columns of doubles (50 data columns and 1 label column), csv format
//        linRegData.csv.mtd  # Metadata file
//        perc.csv            # Used to generate two subsets of the data (for training and testing)
//        perc.csv.mtd        # Metadata file
//        scratch_space       # SystemML scratch_space directory

        // Divide data into two groups
        script = scriptsPath + "/utils/sample.dml";
        args   = new String[] {
                "-f",
                script,
                "-nvargs",
                "X=" + tmp + "/linRegData.csv",
                "sv=perc.csv",
                "O=" + tmp + "/linRegDataParts",
                "ofmt=csv"
        };
        DMLScript.main(args);

//        This script creates two partitions of the original data and places them in a linRegDataParts folder. The files created are as follows:
//
//        linRegDataParts/1       # first partition of data, ~50% of rows of linRegData.csv, csv format
//        linRegDataParts/1.mtd   # metadata
//        linRegDataParts/2       # second partition of data, ~50% of rows of linRegData.csv, csv format
//        linRegDataParts/2.mtd   # metadata

        // split label columns off
        script = scriptsPath + "/utils/splitXY.dml";
        args   = new String[] {
                "-f",
                script,
                "-nvargs",
                "X=" + tmp + "/linRegDataParts/1",
                "y=51",
                "OX=" + tmp + "/linRegData.train.data.csv",
                "OY=" + tmp + "/linRegData.train.labels.csv",
                "ofmt=csv"
        };
        DMLScript.main(args);

        args = new String[] {
                "-f",
                script,
                "-nvargs",
                "X=" +tmp + "/linRegDataParts/2",
                "y=51",
                "OX=" + tmp + "/linRegData.test.data.csv",
                "OY=" + tmp + "/linRegData.test.labels.csv",
                "ofmt=csv"
        };
        DMLScript.main(args);
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        FileUtils.deleteDirectory(tempDir);

        if(tempDir.exists()) {
            throw new IOException("Could not delete temp file: " + tempDir.getAbsolutePath());
        }
    }

    @Test
    public void TrainAndTestModel() throws Exception {
        String tmp     = tempDir.getCanonicalPath();
        File   testDir = testFolder.newFolder("linReg");

        // train model
        String script = scriptsPath + "/algorithms/LinearRegDS.dml";
        String[] args = {
                "-f",
                script,
                "-exec",
                "hybrid_flink",
                "-nvargs",
                "X=" + tmp + "/linRegData.train.data.csv",
                "Y=" + tmp + "/linRegData.train.labels.csv",
                "B=" + testDir + "/betas.csv",
                "fmt=csv"
        };
        DMLScript.main(args);

        // test model
        script = scriptsPath + "/algorithms/GLM-predict.dml";
        args   = new String[] {
                "-f",
                script,
                "-exec",
                "hybrid_spark",
                "-nvargs",
                "X=" + tmp + "/linRegData.test.data.csv",
                "Y=" + tmp + "/linRegData.test.labels.csv",
                "B=" + testDir + "/betas.csv",
                "fmt=csv"
        };
        DMLScript.main(args);
    }
}
