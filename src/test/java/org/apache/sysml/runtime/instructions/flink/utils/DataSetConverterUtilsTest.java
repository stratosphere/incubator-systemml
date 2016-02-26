package org.apache.sysml.runtime.instructions.flink.utils;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.CSVFileFormatProperties;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.junit.Test;

import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DataSetConverterUtilsTest {

    private static final CSVFileFormatProperties CSV_PROPS = new CSVFileFormatProperties(false, ",", false);

    @Test
    public void testCsvToBinaryBlock() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String testFile = getClass().getClassLoader().getResource("flink/haberman.data").getFile();
        DataSource<Tuple2<Integer, String>> input = env.readFile(new RowIndexedInputFormat(), testFile);
        int numRowsPerBlock = 10;
        int numColsPerBlock = 10;

        MatrixCharacteristics mcOut = new MatrixCharacteristics(0L, 0L, numRowsPerBlock, numColsPerBlock);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> mat = DataSetConverterUtils.csvStringToBinaryBlock(env, input, mcOut, false, ",", false, 0.0);

        List<Tuple2<MatrixIndexes, MatrixBlock>> blocks = mat.collect();

        assertEquals(31,blocks.size());
        assertEquals(4, mcOut.getCols());
        assertEquals(306, mcOut.getRows());
        assertEquals(1088, mcOut.getNonZeros());
    }

    @Test
    public void testBinaryBlockToCsv() throws Exception {
        final int rowsPerBlock = 2;
        final int colsPerBlcock = 2;
        MatrixIndexes mIndexes = new MatrixIndexes(1, 1);
        MatrixBlock mBlock = new MatrixBlock(rowsPerBlock, colsPerBlcock, false);
        for (int r = 0; r < rowsPerBlock; r++) {
            for (int c = 0; c < colsPerBlcock; c++) {
                mBlock.setValue(r, c, 23);
            }
        }
        Tuple2<MatrixIndexes, MatrixBlock> binaryBlock = new Tuple2<MatrixIndexes, MatrixBlock>(mIndexes, mBlock);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> binary = env.fromElements(binaryBlock);
        DataSet<String> csv = binary.flatMap(new DataSetConverterUtils.BinaryBlockToCsvFlatMapper(CSV_PROPS));
        csv.print();
    }

    /**
     * Tests the roundtrip from CSV -> Binary Block -> CSV.
     */
    @Test
    public void testCsvToBinaryToCsv() throws Exception {
        URL habermanURL = getClass().getClassLoader().getResource("flink/haberman.data");
        // set colsPerBlock smaller than number of cols to trigger block concatenation
        MatrixCharacteristics mcOut = new MatrixCharacteristics(0L, 0L, 10, 2);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // TODO if Flink supports global sort use this instead
        env.setParallelism(1);
        DataSource<Tuple2<Integer, String>> input = env.readFile(new RowIndexedInputFormat(), habermanURL.getFile());
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> binary = DataSetConverterUtils.csvToBinaryBlock(env, input, mcOut, false, ",", false, 0.0);
        DataSet<String> csv = DataSetConverterUtils.binaryBlockToCsv(binary, mcOut, CSV_PROPS, false);

        List<String> actual = csv.collect();

        List<String> expected = Resources.readLines(habermanURL, Charsets.UTF_8);
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            // SystemML works on doubles, remove the redundant decimal part for this test case
            String actualRow = actual.get(i).replaceAll("\\.[0-9]+", "");
            String expectedRow = expected.get(i);
            assertEquals("content mismatch at row " + i, expectedRow, actualRow);
        }
    }
}
