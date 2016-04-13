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

package org.apache.sysml.runtime.instructions.flink.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

/**
 * Replicates a Vector row- or column-wise
 */
public class ReplicateVectorFunction
        implements FlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>> {
    private static final long serialVersionUID = -1505557561471236851L;

    private final boolean _byRow;
    private final long _numReplicas;

    public ReplicateVectorFunction(boolean byRow, long numReplicas) {
        _byRow = byRow;
        _numReplicas = numReplicas;
    }

    @Override
    public void flatMap(Tuple2<MatrixIndexes, MatrixBlock> value,
                        Collector<Tuple2<MatrixIndexes, MatrixBlock>> out) throws Exception {
        MatrixIndexes ix = value.f0;
        MatrixBlock mb = value.f1;

        //sanity check inputs
        if (_byRow && (ix.getRowIndex() != 1 || mb.getNumRows() > 1)) {
            throw new Exception("Expected a row vector in ReplicateVector");
        }
        if (!_byRow && (ix.getColumnIndex() != 1 || mb.getNumColumns() > 1)) {
            throw new Exception("Expected a column vector in ReplicateVector");
        }

        for (int i = 1; i <= _numReplicas; i++) {
            if (_byRow) {
                out.collect(new Tuple2<MatrixIndexes, MatrixBlock>(new MatrixIndexes(i, ix.getColumnIndex()), mb));
            } else {
                out.collect(new Tuple2<MatrixIndexes, MatrixBlock>(new MatrixIndexes(ix.getRowIndex(), i), mb));
            }
        }

        out.close();
    }
}
