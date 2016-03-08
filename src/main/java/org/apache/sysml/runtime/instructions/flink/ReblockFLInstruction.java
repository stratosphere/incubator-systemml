package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.hops.recompile.Recompiler;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.spark.data.RDDProperties;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.MatrixFormatMetaData;
import org.apache.sysml.runtime.matrix.data.InputInfo;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.Operator;

public class ReblockFLInstruction extends UnaryFLInstruction {
    private int brlen;
    private int bclen;
    private boolean outputEmptyBlocks;

    public ReblockFLInstruction(Operator op, CPOperand in, CPOperand out, int br, int bc, boolean emptyBlocks,
                                String opcode, String instr)
    {
        super(op, in, out, opcode, instr);
        brlen=br;
        bclen=bc;
        outputEmptyBlocks = emptyBlocks;
    }

    public static ReblockFLInstruction parseInstruction(String str)  throws DMLRuntimeException
    {
        String parts[] = InstructionUtils.getInstructionPartsWithValueType(str);
        String opcode = parts[0];

        if(opcode.compareTo("rblk") != 0) {
            throw new DMLRuntimeException("Incorrect opcode for ReblockSPInstruction:" + opcode);
        }

        CPOperand in = new CPOperand(parts[1]);
        CPOperand out = new CPOperand(parts[2]);
        int brlen=Integer.parseInt(parts[3]);
        int bclen=Integer.parseInt(parts[4]);
        boolean outputEmptyBlocks = Boolean.parseBoolean(parts[5]);

        Operator op = null; // no operator for ReblockSPInstruction
        return new ReblockFLInstruction(op, in, out, brlen, bclen, outputEmptyBlocks, opcode, str);
    }

    @Override
    public void processInstruction(ExecutionContext ec) throws DMLRuntimeException, DMLUnsupportedOperationException {
        FlinkExecutionContext flec = (FlinkExecutionContext) ec;

        //set the output characteristics
        MatrixObject mo = flec.getMatrixObject(input1.getName());
        MatrixCharacteristics mc = flec.getMatrixCharacteristics(input1.getName());
        MatrixCharacteristics mcOut = flec.getMatrixCharacteristics(output.getName());
        mcOut.set(mc.getRows(), mc.getCols(), brlen, bclen, mc.getNonZeros());

        //get the source format form the meta data
        MatrixFormatMetaData iimd = (MatrixFormatMetaData) mo.getMetaData();
        if(iimd == null) {
            throw new DMLRuntimeException("Error: Metadata not found");
        }

        //TODO check for in-memory reblock

        if(iimd.getInputInfo() == InputInfo.TextCellInputInfo || iimd.getInputInfo() == InputInfo.MatrixMarketInputInfo ) {

        }
        else if(iimd.getInputInfo() == InputInfo.CSVInputInfo) {
            RDDProperties properties = mo.getRddProperties();
            CSVReblockFLInstruction csvInstruction;
            boolean hasHeader = false;
            String delim = ",";
            boolean fill = false;
            double missingValue = 0;
            if (properties != null) {
                hasHeader = properties.isHasHeader();
                delim = properties.getDelim();
                fill = properties.isFill();
                missingValue = properties.getMissingValue();
            }

            csvInstruction = new CSVReblockFLInstruction(null, input1, output, mcOut.getRowsPerBlock(), mcOut.getColsPerBlock(),
                    hasHeader, delim, fill, missingValue, "csvreblk", instString);
            csvInstruction.processInstruction(flec);
        }
        else if(iimd.getInputInfo()==InputInfo.BinaryCellInputInfo) {
            //TODO
        }
        else if(iimd.getInputInfo()== InputInfo.BinaryBlockInputInfo)
        {
            //TODO
        }
        else {
            throw new DMLRuntimeException("The given InputInfo is not implemented for ReblockSPInstruction:" + iimd.getInputInfo());
        }

    }
}
