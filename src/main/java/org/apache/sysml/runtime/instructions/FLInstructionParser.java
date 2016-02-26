package org.apache.sysml.runtime.instructions;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.instructions.flink.CheckpointFLInstruction;
import org.apache.sysml.runtime.instructions.flink.FLInstruction;
import org.apache.sysml.runtime.instructions.flink.FLInstruction.FLINSTRUCTION_TYPE;
import org.apache.sysml.runtime.instructions.flink.ReblockFLInstruction;
import org.apache.sysml.runtime.instructions.flink.TsmmFLInstruction;

import java.util.HashMap;

public class FLInstructionParser extends InstructionParser {
    public static final HashMap<String, FLINSTRUCTION_TYPE> String2FLInstructionType;
    static {
        String2FLInstructionType = new HashMap<String, FLINSTRUCTION_TYPE>();

        //unary aggregate operators

        //binary aggregate operators (matrix multiplication operators)
        String2FLInstructionType.put( "tsmm" , FLINSTRUCTION_TYPE.TSMM);

        // REBLOCK Instruction Opcodes
        String2FLInstructionType.put( "rblk" , FLINSTRUCTION_TYPE.Reblock);
    }

    public static FLInstruction parseSingleInstruction(String str)
        throws DMLRuntimeException, DMLUnsupportedOperationException
    {
        if ( str == null || str.isEmpty() )
            return null;

        FLINSTRUCTION_TYPE cptype = InstructionUtils.getFLType(str);
        if ( cptype == null )
            throw new DMLUnsupportedOperationException("Invalid FL Instruction Type: " + str);
        FLInstruction flinst = parseSingleInstruction(cptype, str);
        if ( flinst == null )
            throw new DMLRuntimeException("Unable to parse instruction: " + str);
        return flinst;
    }

    public static FLInstruction parseSingleInstruction(FLINSTRUCTION_TYPE fltype, String str)
        throws DMLRuntimeException, DMLUnsupportedOperationException
    {
        if (str == null || str.isEmpty())
            return null;

        String[] parts = null;
        switch (fltype) {
            case Reorg:
                return CheckpointFLInstruction.parseInstruction(str);
            case TSMM:
                return TsmmFLInstruction.parseInstruction(str);
            case Reblock:
                return ReblockFLInstruction.parseInstruction(str);
            default:
                throw new DMLUnsupportedOperationException("Invalid FL Instruction Type: " + fltype);
        }
    }
}
