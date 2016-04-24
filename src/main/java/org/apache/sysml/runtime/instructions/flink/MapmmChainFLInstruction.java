/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.lops.MapMultChain;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

import static org.apache.sysml.runtime.instructions.flink.functions.MatrixMultiplicationFunctions.*;

/**
 * Implementation of {@link org.apache.sysml.runtime.instructions.Instruction} to run chained matrix multiplication on
 * Flink. This is the low-level implementation for expressions of the form:
 * <p>
 * <ol>
 * <li><pre><code>
 *     X = ... # Matrix
 *     v = ... # Vector
 *     c = t(X) %*% (X %*% v)
 * </code></pre></li>
 * <p>
 * <li><pre><code>
 *     X = ... # Matrix
 *     v = ... # Vector
 *     w = ... # Vector
 *     c = t(X) %*% (w %*% (X %*% v))
 * </code></pre></li>
 * <p>
 * <li><pre><code>
 *     X = ... # Matrix
 *     v = ... # Vector
 *     y = ... # Vector
 *     c = t(x) %*% (( X %*% v) - y)
 * </code></pre></li>
 * </ol>
 * <p>
 * It is expected that matrix <code>X</code> and vector <code>y</code> are row-partitioned, and <code>v</code> is
 * a unpartitioned vector. That is a single block of <code>X</code> contains at least one complete row of <code>X</code>.
 * Basically a single block of <code>X</code>, all of <code>v</code> and <code>y</code> fit into memory at once on a
 * single node.
 */
public class MapmmChainFLInstruction extends FLInstruction {

    /**
     * Input matrix, one row per block
     */
    private final CPOperand inputX;
    /**
     * First vector, one block only
     */
    private final CPOperand inputV;
    /**
     * Second vector, tiny but partitioned
     * might be null
     */
    private final CPOperand inputW;

    private final CPOperand output;

    private final MapMultChain.ChainType chainType;

    public MapmmChainFLInstruction(
            CPOperand inputX, CPOperand inputV, CPOperand output, MapMultChain.ChainType chainType,
            String opcode, String instr) {
        this(inputX, inputV, null, output, chainType, opcode, instr);
    }

    public MapmmChainFLInstruction(
            CPOperand inputX, CPOperand inputV, CPOperand inputW, CPOperand output, MapMultChain.ChainType chainType,
            String opcode, String instr) {
        super(opcode, instr);
        this.inputX = inputX;
        this.inputV = inputV;
        this.inputW = inputW;
        this.output = output;
        this.chainType = chainType;
    }

    public static MapmmChainFLInstruction parseInstruction( String str ) throws DMLRuntimeException {
        String[] parts = InstructionUtils.getInstructionPartsWithValueType( str );
        InstructionUtils.checkNumFields ( parts, 4, 5 );
        String opcode = parts[0];

        //check supported opcode
        if ( !opcode.equalsIgnoreCase(MapMultChain.OPCODE)){
            throw new DMLRuntimeException("MapmmChainSPInstruction.parseInstruction():: Unknown opcode " + opcode);
        }

        //parse instruction parts (without exec type)
        CPOperand in1 = new CPOperand(parts[1]);
        CPOperand in2 = new CPOperand(parts[2]);

        if( parts.length==5 )
        {
            CPOperand out = new CPOperand(parts[3]);
            MapMultChain.ChainType type = MapMultChain.ChainType.valueOf(parts[4]);

            return new MapmmChainFLInstruction(in1, in2, out, type, opcode, str);
        }
        else //parts.length==6
        {
            CPOperand in3 = new CPOperand(parts[3]);
            CPOperand out = new CPOperand(parts[4]);
            MapMultChain.ChainType type = MapMultChain.ChainType.valueOf(parts[5]);

            return new MapmmChainFLInstruction(in1, in2, in3, out, type, opcode, str);
        }
    }

    @Override
    public void processInstruction(ExecutionContext ec) throws DMLRuntimeException {
        assert ec instanceof FlinkExecutionContext :
                "Expected " + FlinkExecutionContext.class.getCanonicalName() + " got " + ec.getClass().getCanonicalName();
        FlinkExecutionContext fec = (FlinkExecutionContext) ec;

        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out;

        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> X = fec.getBinaryBlockDataSetHandleForVariable(inputX.getName());
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> v = fec.getBinaryBlockDataSetHandleForVariable(inputV.getName());

        // XtXv
        if (chainType == MapMultChain.ChainType.XtXv) {
            out = X.map(new MultiplyTransposedMatrixBlocks(chainType, "v"))
                    .withBroadcastSet(v, "v")
                    .reduce(new SumMatrixBlocksStable());
        }

        // XtwXv or XtXvy
        else {
            DataSet<Tuple2<MatrixIndexes, MatrixBlock>> w = fec.getBinaryBlockDataSetHandleForVariable(
                    inputW.getName());
            out = X.join(w, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
                    .where(new RowSelector())
                    .equalTo(new RowSelector())
                    .with(new MultiplyTransposedMatrixBlocksWithVector(chainType, "v"))
                    .withBroadcastSet(v, "v")
                    .reduce(new SumMatrixBlocksStable());
        }

        fec.setDataSetHandleForVariable(output.getName(), out);
    }
}
