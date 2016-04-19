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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.storage.StorageLevel;
import org.apache.sysml.runtime.DMLRuntimeException;
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

    public CheckpointFLInstruction(Operator op, CPOperand in, CPOperand out, StorageLevel level, String opcode,
                                   String istr) {
        super(op, in, out, opcode, istr);
        _fltype = FLInstruction.FLINSTRUCTION_TYPE.Reorg;
        _level = level;
    }

    public static CheckpointFLInstruction parseInstruction(String str)
            throws DMLRuntimeException {
        String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
        InstructionUtils.checkNumFields(parts, 2); //TODO actually 3

        String opcode = parts[0];
        CPOperand in = new CPOperand(parts[1]);
        CPOperand out = new CPOperand(parts[2]);

        //StorageLevel level = StorageLevel.fromString(parts[3]);

        return new CheckpointFLInstruction(null, in, out, null, opcode, str);
    }

    @Override
    public void processInstruction(ExecutionContext ec)
            throws DMLRuntimeException {

        FlinkExecutionContext flec = (FlinkExecutionContext) ec;

        //get input dataset handle
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> in = flec.getBinaryBlockDataSetHandleForVariable(input1.getName());
        MatrixCharacteristics mcIn = flec.getMatrixCharacteristics(input1.getName());

        // Step 2: Checkpoint given rdd (only if currently in different storage level to prevent redundancy)
        // -------
        // Note that persist is an transformation which will be triggered on-demand with the next rdd operations
        // This prevents unnecessary overhead if the dataset is only consumed by cp operations.

        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out = null;
        // TODO since flink has not persist primitive, we can not do the checkpointing
        out = in; //pass-through
        MatrixObject mo = flec.getMatrixObject(input1.getName());
        flec.setVariable(output.getName(), mo);
    }
}
