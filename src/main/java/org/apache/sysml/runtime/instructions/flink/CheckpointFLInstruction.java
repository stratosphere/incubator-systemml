package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.storage.StorageLevel;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.Operator;

public class CheckpointFLInstruction extends UnaryFLInstruction {

    //default storage level
    private StorageLevel _level = null;

    public CheckpointFLInstruction(Operator op, CPOperand in, CPOperand out, StorageLevel level, String opcode, String istr){
        super(op, in, out, opcode, istr);
        _fltype = FLInstruction.FLINSTRUCTION_TYPE.Reorg;
        _level  = level;
    }

    public static CheckpointFLInstruction parseInstruction ( String str )
            throws DMLRuntimeException
    {
        String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
        InstructionUtils.checkNumFields(parts, 3);

        String opcode = parts[0];
        CPOperand in = new CPOperand(parts[1]);
        CPOperand out = new CPOperand(parts[2]);

        StorageLevel level = StorageLevel.fromString(parts[3]);

        return new CheckpointFLInstruction(null, in, out, level, opcode, str);
    }

    @Override
    public void processInstruction(ExecutionContext ec)
            throws DMLUnsupportedOperationException, DMLRuntimeException {

        FlinkExecutionContext flec = (FlinkExecutionContext) ec;

        //get input dataset handle
        DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in = flec.getBinaryBlockDataSetHandleForVariable(input1.getName());
        MatrixCharacteristics mcIn = flec.getMatrixCharacteristics(input1.getName());

        // Step 2: Checkpoint given rdd (only if currently in different storage level to prevent redundancy)
        // -------
        // Note that persist is an transformation which will be triggered on-demand with the next rdd operations
        // This prevents unnecessary overhead if the dataset is only consumed by cp operations.

        DataSet<Tuple2<MatrixIndexes,MatrixBlock>> out = null;
        // TODO since flink has not persist primitive, we can not do the checkpointing
        out = in; //pass-through
        MatrixObject mo = flec.getMatrixObject( input1.getName() );
        flec.setVariable( output.getName(), mo);
    }
}
