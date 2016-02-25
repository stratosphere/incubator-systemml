package org.apache.sysml.runtime.instructions.flink;

import org.apache.sysml.hops.recompile.Recompiler;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.MatrixFormatMetaData;
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

        //check for in-memory reblock (w/ lazy spark context, potential for latency reduction)
        if( Recompiler.checkCPReblock(flec, input1.getName()) ) {
            Recompiler.executeInMemoryReblock(flec, input1.getName(), output.getName());
            return;
        }

    }
}
