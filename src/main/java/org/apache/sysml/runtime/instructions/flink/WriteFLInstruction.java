package org.apache.sysml.runtime.instructions.flink;

import org.apache.commons.io.IOExceptionWithCause;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetConverterUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.*;
import org.apache.sysml.runtime.util.MapReduceTool;

import java.io.IOException;

public class WriteFLInstruction extends FLInstruction {
    private CPOperand input1 = null;
    private CPOperand input2 = null;
    private CPOperand input3 = null;
    private FileFormatProperties formatProperties;

    //scalars might occur for transform
    private boolean isInputMatrixBlock = true;

    public WriteFLInstruction(String opcode, String istr) {
        super(opcode, istr);
    }

    public WriteFLInstruction(CPOperand in1, CPOperand in2, CPOperand in3, String opcode, String str) {
        super(opcode, str);
        input1 = in1;
        input2 = in2;
        input3 = in3;

        formatProperties = null; // set in case of csv
    }

    public static WriteFLInstruction parseInstruction(String str)
            throws DMLRuntimeException {
        String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
        String opcode = parts[0];

        if (!opcode.equals("write")) {
            throw new DMLRuntimeException("Unsupported opcode");
        }

        // All write instructions have 3 parameters, except in case of delimited/csv file.
        // Write instructions for csv files also include three additional parameters (hasHeader, delimiter, sparse)
        if (parts.length != 4 && parts.length != 8) {
            throw new DMLRuntimeException("Invalid number of operands in write instruction: " + str);
        }

        //FLINK°write°_mVar2·MATRIX·DOUBLE°./src/test/scripts/functions/data/out/B·SCALAR·STRING·true°matrixmarket·SCALAR·STRING·true
        // _mVar2·MATRIX·DOUBLE
        CPOperand in1 = null, in2 = null, in3 = null;
        in1 = new CPOperand(parts[1]);
        in2 = new CPOperand(parts[2]);
        in3 = new CPOperand(parts[3]);

        WriteFLInstruction inst = new WriteFLInstruction(in1, in2, in3, opcode, str);

        if (in3.getName().equalsIgnoreCase("csv")) {
            boolean hasHeader = Boolean.parseBoolean(parts[4]);
            String delim = parts[5];
            boolean sparse = Boolean.parseBoolean(parts[6]);
            FileFormatProperties formatProperties = new CSVFileFormatProperties(hasHeader, delim, sparse);
            inst.setFormatProperties(formatProperties);

            boolean isInputMB = Boolean.parseBoolean(parts[7]);
            inst.setInputMatrixBlock(isInputMB);
        }
        return inst;
    }


    public FileFormatProperties getFormatProperties() {
        return formatProperties;
    }

    public void setFormatProperties(FileFormatProperties prop) {
        formatProperties = prop;
    }

    public void setInputMatrixBlock(boolean isMB) {
        isInputMatrixBlock = isMB;
    }

    public boolean isInputMatrixBlock() {
        return isInputMatrixBlock;
    }

    @Override
    public void processInstruction(ExecutionContext ec)
            throws DMLRuntimeException, DMLUnsupportedOperationException {
        FlinkExecutionContext flec = (FlinkExecutionContext) ec;

        //get filename (literal or variable expression)
        String fname = ec.getScalarInput(input2.getName(), Expression.ValueType.STRING, input2.isLiteral()).getStringValue();

        try {
            //if the file already exists on HDFS, remove it.
            MapReduceTool.deleteFileIfExistOnHDFS(fname);
        } catch (IOException ioe) {
            throw new DMLRuntimeException("Could not delete file on hdfs: " + fname);
        }

        //prepare output info according to meta data
        String outFmt = input3.getName();
        OutputInfo oi = OutputInfo.stringToOutputInfo(outFmt);

        //get dataset
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> in1 = flec.getBinaryBlockDataSetHandleForVariable(input1.getName());
        MatrixCharacteristics mc = flec.getMatrixCharacteristics(input1.getName());

        // TODO matrixmarket && Textcell output
        if (oi == OutputInfo.CSVOutputInfo) {
            DataSet<String> out;

            if (isInputMatrixBlock) {
                //TODO compute nnz values
                out = DataSetConverterUtils.binaryBlockToCsv(in1, mc, (CSVFileFormatProperties) formatProperties, true);
            } else {
                //TODO: This case is applicable when the CSV output from transform() is written out
            }
        }
        //TODO binaryblock output
        else {
            //unsupported formats: binarycell (not externalized)
            throw new DMLRuntimeException("Unexpected data format: " + outFmt);
        }
        // write meta data file
        try {
            MapReduceTool.writeMetaDataFile(fname + ".mtd", Expression.ValueType.DOUBLE, mc, oi, formatProperties);
        } catch (IOException ioe) {
            throw new DMLRuntimeException("Could not write metadata-file for output " + fname, ioe);
        }
    }
}
