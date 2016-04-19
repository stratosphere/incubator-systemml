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

package org.apache.sysml.runtime.instructions;

import org.apache.sysml.lops.Checkpoint;
import org.apache.sysml.lops.DataGen;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.flink.*;
import org.apache.sysml.runtime.instructions.flink.FLInstruction.FLINSTRUCTION_TYPE;

import java.util.HashMap;

public class FLInstructionParser extends InstructionParser {
    public static final HashMap<String, FLINSTRUCTION_TYPE> String2FLInstructionType;

    static {
        String2FLInstructionType = new HashMap<String, FLINSTRUCTION_TYPE>();

        //unary aggregate operators
        String2FLInstructionType.put("uak+", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uark+", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uack+", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uasqk+", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uarsqk+", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uacsqk+", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uamean", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uarmean", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uacmean", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uavar", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uarvar", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uacvar", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uamax", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uarmax", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uarimax", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uacmax", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uamin", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uarmin", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uarimin", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uacmin", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("ua+", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uar+", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uac+", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("ua*", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uatrace", FLINSTRUCTION_TYPE.AggregateUnary);
        String2FLInstructionType.put("uaktrace", FLINSTRUCTION_TYPE.AggregateUnary);


        //binary aggregate operators (matrix multiplication operators)
        String2FLInstructionType.put("mapmm", FLINSTRUCTION_TYPE.MAPMM);
        String2FLInstructionType.put("mapmmchain", FLINSTRUCTION_TYPE.MAPMMCHAIN);
        String2FLInstructionType.put("tsmm", FLINSTRUCTION_TYPE.TSMM);
        String2FLInstructionType.put("cpmm", FLINSTRUCTION_TYPE.CPMM);

        // REBLOCK Instruction Opcodes
        String2FLInstructionType.put("rblk", FLINSTRUCTION_TYPE.Reblock);
        String2FLInstructionType.put("csvrblk", FLINSTRUCTION_TYPE.CSVReblock);

        String2FLInstructionType.put(DataGen.RAND_OPCODE, FLINSTRUCTION_TYPE.Rand);

        // Spark-specific instructions
        String2FLInstructionType.put(Checkpoint.OPCODE, FLINSTRUCTION_TYPE.Checkpoint);

        String2FLInstructionType.put("write", FLINSTRUCTION_TYPE.Write);

		// Reorg Instruction Opcodes (repositioning of existing values)
		String2FLInstructionType.put( "r'"   	   , FLINSTRUCTION_TYPE.Reorg);
		String2FLInstructionType.put( "rev"   	   , FLINSTRUCTION_TYPE.Reorg);
		String2FLInstructionType.put( "rdiag"      , FLINSTRUCTION_TYPE.Reorg);
		String2FLInstructionType.put( "rsort"      , FLINSTRUCTION_TYPE.Reorg);

        // ArithmeticBinary
        String2FLInstructionType.put("+", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("-", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("*", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("/", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("%%", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("%/%", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("1-*", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("^", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("^2", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("*2", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("map+", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("map-", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("map*", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("map/", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("map%%", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("map%/%", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("map1-*", FLINSTRUCTION_TYPE.ArithmeticBinary);
        String2FLInstructionType.put("map^", FLINSTRUCTION_TYPE.ArithmeticBinary);

        String2FLInstructionType.put("write", FLINSTRUCTION_TYPE.Write);

    }

    public static FLInstruction parseSingleInstruction(String str)
            throws DMLRuntimeException {
        if (str == null || str.isEmpty())
            return null;

        FLINSTRUCTION_TYPE cptype = InstructionUtils.getFLType(str);
        if (cptype == null)
            throw new DMLRuntimeException("Invalid FL Instruction Type: " + str);
        FLInstruction flinst = parseSingleInstruction(cptype, str);
        if (flinst == null)
            throw new DMLRuntimeException("Unable to parse instruction: " + str);
        return flinst;
    }

    public static FLInstruction parseSingleInstruction(FLINSTRUCTION_TYPE fltype, String str)
            throws DMLRuntimeException {
        if (str == null || str.isEmpty())
            return null;

        String[] parts = null;
        switch (fltype) {
            case AggregateUnary:
                return AggregateUnaryFLInstruction.parseInstruction(str);
            case Reorg:
                return CheckpointFLInstruction.parseInstruction(str);
            case ArithmeticBinary:
                return ArithmeticBinaryFLInstruction.parseInstruction(str);

            // matrix multiplication instructions
            /*
			case CPMM:
				return CpmmFLInstruction.parseInstruction(str);*/
            case MAPMM:
                return MapmmFLInstruction.parseInstruction(str);
			/*case MAPMMCHAIN:
				return MapmmChainFLInstruction.parseInstruction(str);*/
            case TSMM:
                return TsmmFLInstruction.parseInstruction(str);


            case Reblock:
                return ReblockFLInstruction.parseInstruction(str);
            case CSVReblock:
                return CSVReblockFLInstruction.parseInstruction(str);
            case Write:
                return WriteFLInstruction.parseInstruction(str);
            case Checkpoint:
                return CheckpointFLInstruction.parseInstruction(str);
            case Rand:
                return RandFLInstruction.parseInstruction(str);


            case INVALID:
            default:
                throw new DMLRuntimeException("Invalid FL Instruction Type: " + fltype);
        }
    }
}
