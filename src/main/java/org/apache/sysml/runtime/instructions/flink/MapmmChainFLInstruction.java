package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.lops.MapMultChain;
import org.apache.sysml.lops.MapMultChain.ChainType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetAggregateUtils;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.Operator;

import static org.apache.sysml.runtime.instructions.flink.functions.MatrixMultiplicationFunctions.*;

/**
 * Implementation of {@link org.apache.sysml.runtime.instructions.Instruction} to run chained matrix multiplication on
 * Flink. This is the low-level implementation for expressions of the form:
 *
 * <ol>
 * <li><pre><code>
 *     X = ... # Matrix
 *     v = ... # Vector
 *     c = t(X) %*% (X %*% v)
 * </code></pre></li>
 *
 * <li><pre><code>
 *     X = ... # Matrix
 *     v = ... # Vector
 *     w = ... # Vector
 *     c = t(X) %*% (w %*% (X %*% v))
 * </code></pre></li>
 *
 * <li><pre><code>
 *     X = ... # Matrix
 *     v = ... # Vector
 *     y = ... # Vector
 *     c = t(x) %*% (( X %*% v) - y)
 * </code></pre></li>
 * </ol>
 *
 * It is expected that matrix <code>X</code> and vector <code>y</code> are row-partitioned, and <code>v</code> is
 * a unpartitioned vector. That is a single block of <code>X</code> contains at least one complete row of <code>X</code>.
 * Basically a single block of <code>X</code>, all of <code>v</code> and <code>y</code> fit into memory at once on a
 * single node.
 */
public class MapmmChainFLInstruction extends FLInstruction {

    // Input matrix, one row per block
    private CPOperand input1;
    // First vector, one block only
    private CPOperand input2;
    //Second vector, tiny but partitioned, might be null
    private CPOperand input3;
    private CPOperand output;

    private ChainType chainType;

    public MapmmChainFLInstruction(
            Operator op, CPOperand in1, CPOperand in2, CPOperand out,
            ChainType type, String opcode, String istr)
    {
        super(op, opcode, istr);
        _fltype = FLINSTRUCTION_TYPE.MAPMMCHAIN;

        input1 = in1;
        input2 = in2;
        output = out;

        chainType = type;
    }

    public MapmmChainFLInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand out,
                                   ChainType type, String opcode, String istr)
    {
        super(op, opcode, istr);
        _fltype = FLINSTRUCTION_TYPE.MAPMMCHAIN;

        input1 = in1;
        input2 = in2;
        input3 = in3;

        chainType = type;
    }

    public MapmmChainFLInstruction parseInstruction(String istr) throws DMLRuntimeException {
        String[] parts = InstructionUtils.getInstructionPartsWithValueType(istr);
        InstructionUtils.checkNumFields(parts, 4, 5);
        String opcode = parts[0];

        if (!opcode.equalsIgnoreCase(MapMultChain.OPCODE)) {
            throw new DMLRuntimeException("MapmmChainFLInstruction.parseInstruction():: Unknown opcode " + opcode);
        }

        // parse instruction  parts (without exec type)
        CPOperand in1 = new CPOperand(parts[1]);
        CPOperand in2 = new CPOperand(parts[2]);

        if (parts.length == 5) {
            CPOperand out = new CPOperand(parts[3]);
            ChainType type = ChainType.valueOf(parts[4]);

            return new MapmmChainFLInstruction(null, in1, in2, out, type, opcode, istr);
        } else if (parts.length == 6) {
            CPOperand in3 = new CPOperand(parts[3]);
            CPOperand out = new CPOperand(parts[4]);
            ChainType type = ChainType.valueOf(parts[5]);

            return new MapmmChainFLInstruction(null, in1, in2, in3, out, type, opcode, istr);
        } else {
            throw new DMLRuntimeException("MapmmChainFLInstruction.parseInstruction():: wrong number of parts " + opcode);
        }
    }

    @Override
    public void processInstruction(ExecutionContext ec) throws DMLRuntimeException {
        FlinkExecutionContext fec = (FlinkExecutionContext) ec;

        // get dataset handles for inputs (input2 is a broadcast!)
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> inX = fec.getBinaryBlockDataSetHandleForVariable(input1.getName());
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> inV = fec.getBinaryBlockDataSetHandleForVariable(input2.getName());

        //execute mapmmchain (guaranteed to have single output block)
        MatrixBlock out = null;
        // XtXv
        if (chainType == MapMultChain.ChainType.XtXv) {
            DataSet<Tuple2<MatrixIndexes, MatrixBlock>> tmp = inX.map(new MultiplyTransposedMatrixBlocks(chainType, "v"))
                                                                    .withBroadcastSet(inV, "v");
            out = DataSetAggregateUtils.sumStable1(tmp);
        }
        else { // XtwXv or XtXvy
            DataSet<Tuple2<MatrixIndexes, MatrixBlock>> w = fec.getBinaryBlockDataSetHandleForVariable(input3.getName());
            DataSet<Tuple2<MatrixIndexes, MatrixBlock>> tmp = inX.join(w)
                                                                    .where(new RowSelector())
                                                                    .equalTo(new RowSelector())
                                                                    .with(new MultiplyTransposedMatrixBlocksWithVector(chainType, "v"))
                                                                    .withBroadcastSet(inV, "v");
            out = DataSetAggregateUtils.sumStable1(tmp);
        }

        //put output block into symbol table (no lineage because single block)
        //this also includes implicit maintenance of matrix characteristics
        fec.setMatrixOutput(output.getName(), out);
    }
}
