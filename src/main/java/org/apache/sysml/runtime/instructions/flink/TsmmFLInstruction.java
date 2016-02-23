package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.lops.MMTSJ.MMTSJType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetAggregateUtils;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.Operator;

public class TsmmFLInstruction {

    private MMTSJType _type = null;
    private FLInstruction.FLINSTRUCTION_TYPE _fltype = null;

    public TsmmFLInstruction() {
        //super(op, in1, out, opcode, istr);
        _fltype = FLInstruction.FLINSTRUCTION_TYPE.TSMM;
        _type   = MMTSJType.LEFT;
    }

    public static TsmmFLInstruction parseInstruction(String str) throws DMLRuntimeException {
        String[] parts  = InstructionUtils.getInstructionPartsWithValueType(str);
        String   opcode = parts[0];

        //check supported opcode
        if ( !opcode.equalsIgnoreCase("tsmm") ) {
            throw new DMLRuntimeException("TsmmFLInstruction.parseInstruction():: Unknown opcode " + opcode);
        }

        CPOperand in1 = new CPOperand(parts[1]);
        CPOperand out = new CPOperand(parts[2]);
        MMTSJType type = MMTSJType.valueOf(parts[3]);

        return new TsmmFLInstruction();
    }

    //FIXME this is not how instructions should be implemented but only a demonstration!
    public MatrixBlock processInstruction(DataSet<Tuple2<MatrixIndexes, MatrixBlock>> input) throws DMLRuntimeException, DMLUnsupportedOperationException {
        DataSet<MatrixBlock> tmp = input.map(new DataSetTSMMFunction(_type));
        return DataSetAggregateUtils.sumStable(tmp);
    }

    private static class DataSetTSMMFunction implements MapFunction<Tuple2<MatrixIndexes, MatrixBlock>,MatrixBlock> {

        private static final long serialVersionUID = 2935770425858019666L;

        private MMTSJType _type = null;

        public DataSetTSMMFunction( MMTSJType type ) {
            _type = type;
        }

        @Override
        public MatrixBlock map(Tuple2<MatrixIndexes, MatrixBlock> value) throws Exception {
            return value.f1.transposeSelfMatrixMultOperations(new MatrixBlock(), _type);
        }
    }
}
