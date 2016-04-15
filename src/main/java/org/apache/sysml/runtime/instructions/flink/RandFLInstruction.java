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

import org.apache.commons.math3.random.Well1024a;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.hops.DataGenOp;
import org.apache.sysml.hops.Hop;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.lops.DataGen;
import org.apache.sysml.lops.Lop;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.LibMatrixDatagen;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.RandomMatrixGenerator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.util.UtilFunctions;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class RandFLInstruction extends UnaryFLInstruction {
    //internal configuration
    private static final long INMEMORY_NUMBLOCKS_THRESHOLD = 1024 * 1024;

    private Hop.DataGenMethod method = Hop.DataGenMethod.INVALID;

    private long rows;
    private long cols;
    private int rowsInBlock;
    private int colsInBlock;
    private double minValue;
    private double maxValue;
    private double sparsity;
    private String pdf;
    private String pdfParams;
    private long seed = 0;
    private String dir;
    private double seq_from;
    private double seq_to;
    private double seq_incr;

    //sample specific attributes
    private boolean replace;

    public RandFLInstruction(Operator op, Hop.DataGenMethod mthd, CPOperand in, CPOperand out, long rows, long cols,
                             int rpb, int cpb, double minValue, double maxValue, double sparsity, long seed, String dir,
                             String probabilityDensityFunction, String pdfParams, String opcode, String istr) {
        super(op, in, out, opcode, istr);

        this.method = mthd;
        this.rows = rows;
        this.cols = cols;
        this.rowsInBlock = rpb;
        this.colsInBlock = cpb;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.sparsity = sparsity;
        this.seed = seed;
        this.dir = dir;
        this.pdf = probabilityDensityFunction;
        this.pdfParams = pdfParams;

    }

    public RandFLInstruction(Operator op, Hop.DataGenMethod mthd, CPOperand in, CPOperand out,
                             long rows, long cols, int rpb, int cpb, double seqFrom,
                             double seqTo, double seqIncr, String opcode, String istr) {
        super(op, in, out, opcode, istr);
        this.method = mthd;
        this.rows = rows;
        this.cols = cols;
        this.rowsInBlock = rpb;
        this.colsInBlock = cpb;
        this.seq_from = seqFrom;
        this.seq_to = seqTo;
        this.seq_incr = seqIncr;
    }

    public RandFLInstruction(Operator op, Hop.DataGenMethod mthd, CPOperand in,
                             CPOperand out, long rows, long cols, int rpb, int cpb,
                             double maxValue, boolean replace, long seed, String opcode,
                             String istr) {
        super(op, in, out, opcode, istr);

        this.method = mthd;
        this.rows = rows;
        this.cols = cols;
        this.rowsInBlock = rpb;
        this.colsInBlock = cpb;
        this.maxValue = maxValue;
        this.replace = replace;
        this.seed = seed;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public long getCols() {
        return cols;
    }

    public void setCols(long cols) {
        this.cols = cols;
    }

    public int getRowsInBlock() {
        return rowsInBlock;
    }

    public void setRowsInBlock(int rowsInBlock) {
        this.rowsInBlock = rowsInBlock;
    }

    public int getColsInBlock() {
        return colsInBlock;
    }

    public void setColsInBlock(int colsInBlock) {
        this.colsInBlock = colsInBlock;
    }

    public double getMinValue() {
        return minValue;
    }

    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(double maxValue) {
        this.maxValue = maxValue;
    }

    public double getSparsity() {
        return sparsity;
    }

    public void setSparsity(double sparsity) {
        this.sparsity = sparsity;
    }

    public static RandFLInstruction parseInstruction(String str)
            throws DMLRuntimeException {
        String[] s = InstructionUtils.getInstructionPartsWithValueType(str);
        String opcode = s[0];

        Hop.DataGenMethod method = Hop.DataGenMethod.INVALID;
        if (opcode.equalsIgnoreCase(DataGen.RAND_OPCODE)) {
            method = Hop.DataGenMethod.RAND;
            InstructionUtils.checkNumFields(str, 12);
        } else if (opcode.equalsIgnoreCase(DataGen.SEQ_OPCODE)) {
            method = Hop.DataGenMethod.SEQ;
            // 8 operands: rows, cols, rpb, cpb, from, to, incr, outvar
            InstructionUtils.checkNumFields(str, 8);
        } else if (opcode.equalsIgnoreCase(DataGen.SAMPLE_OPCODE)) {
            method = Hop.DataGenMethod.SAMPLE;
            // 7 operands: range, size, replace, seed, rpb, cpb, outvar
            InstructionUtils.checkNumFields(str, 7);
        }

        Operator op = null;
        // output is specified by the last operand
        CPOperand out = new CPOperand(s[s.length - 1]);

        if (method == Hop.DataGenMethod.RAND) {
            long rows = -1, cols = -1;
            if (!s[1].contains(Lop.VARIABLE_NAME_PLACEHOLDER)) {
                rows = Double.valueOf(s[1]).longValue();
            }
            if (!s[2].contains(Lop.VARIABLE_NAME_PLACEHOLDER)) {
                cols = Double.valueOf(s[2]).longValue();
            }

            int rpb = Integer.parseInt(s[3]);
            int cpb = Integer.parseInt(s[4]);

            double minValue = -1, maxValue = -1;
            if (!s[5].contains(Lop.VARIABLE_NAME_PLACEHOLDER)) {
                minValue = Double.valueOf(s[5]).doubleValue();
            }
            if (!s[6].contains(Lop.VARIABLE_NAME_PLACEHOLDER)) {
                maxValue = Double.valueOf(s[6]).doubleValue();
            }

            double sparsity = Double.parseDouble(s[7]);

            long seed = DataGenOp.UNSPECIFIED_SEED;
            if (!s[8].contains(Lop.VARIABLE_NAME_PLACEHOLDER)) {
                seed = Long.parseLong(s[8]);
            }

            String dir = s[9];
            String pdf = s[10];
            String pdfParams = s[11];

            return new RandFLInstruction(op, method, null, out, rows, cols, rpb, cpb, minValue, maxValue, sparsity,
                    seed, dir, pdf, pdfParams, opcode, str);
        } else if (method == Hop.DataGenMethod.SEQ) {
            // Example Instruction: CP:seq:11:1:1000:1000:1:0:-0.1:scratch_space/_p7932_192.168.1.120//_t0/:mVar1
            long rows = Double.valueOf(s[1]).longValue();
            long cols = Double.valueOf(s[2]).longValue();
            int rpb = Integer.parseInt(s[3]);
            int cpb = Integer.parseInt(s[4]);

            double from, to, incr;
            from = to = incr = Double.NaN;
            if (!s[5].contains(Lop.VARIABLE_NAME_PLACEHOLDER)) {
                from = Double.valueOf(s[5]);
            }
            if (!s[6].contains(Lop.VARIABLE_NAME_PLACEHOLDER)) {
                to = Double.valueOf(s[6]);
            }
            if (!s[7].contains(Lop.VARIABLE_NAME_PLACEHOLDER)) {
                incr = Double.valueOf(s[7]);
            }

            CPOperand in = null;
            return new RandFLInstruction(op, method, in, out, rows, cols, rpb, cpb, from, to, incr, opcode, str);
        } else if (method == Hop.DataGenMethod.SAMPLE) {
            // Example Instruction: SPARK:sample:10:100:false:1000:1000:_mVar2·MATRIX·DOUBLE
            double max = 0;
            long rows = 0, cols;
            boolean replace = false;

            if (!s[1].contains(Lop.VARIABLE_NAME_PLACEHOLDER))
                max = Double.valueOf(s[1]);
            if (!s[2].contains(Lop.VARIABLE_NAME_PLACEHOLDER))
                rows = Double.valueOf(s[2]).longValue();
            cols = 1;

            if (!s[3].contains(Lop.VARIABLE_NAME_PLACEHOLDER))
                replace = Boolean.valueOf(s[3]);

            long seed = Long.parseLong(s[4]);
            int rpb = Integer.parseInt(s[5]);
            int cpb = Integer.parseInt(s[6]);

            return new RandFLInstruction(op, method, null, out, rows, cols, rpb, cpb, max, replace, seed, opcode, str);
        } else
            throw new DMLRuntimeException("Unrecognized data generation method: " + method);
    }

    @Override
    public void processInstruction(ExecutionContext ec)
            throws DMLRuntimeException {
        FlinkExecutionContext flec = (FlinkExecutionContext) ec;

        //process specific datagen operator
        switch (method) {
            case RAND:
                generateRandData(flec);
                break;
            //case SEQ: generateSequence(flec); break;
            //case SAMPLE: generateSample(flec); break;
            default:
                throw new DMLRuntimeException("Invalid datagen method: " + method);
        }
    }

    private void generateRandData(FlinkExecutionContext flec) throws DMLRuntimeException {
        //step 1: generate pseudo-random seed (because not specified)
        long lSeed = seed; //seed per invocation
        if (lSeed == DataGenOp.UNSPECIFIED_SEED)
            lSeed = DataGenOp.generateRandomSeed();

        if (LOG.isTraceEnabled())
            LOG.trace("Process RandSPInstruction rand with seed = " + lSeed + ".");

        //step 2: potential in-memory rand operations if applicable
        if (isMemAvail(rows, cols, sparsity, minValue, maxValue)
                && DMLScript.rtplatform != DMLScript.RUNTIME_PLATFORM.FLINK) {
            RandomMatrixGenerator rgen = LibMatrixDatagen.createRandomMatrixGenerator(
                    pdf, (int) rows, (int) cols, rowsInBlock, colsInBlock,
                    sparsity, minValue, maxValue, pdfParams);
            MatrixBlock mb = MatrixBlock.randOperations(rgen, lSeed);

            flec.setMatrixOutput(output.getName(), mb);
        }

        //step 3: seed generation
        DataSet<Tuple3<MatrixIndexes, Long, Long>> seedsData;
        Well1024a bigrand = LibMatrixDatagen.setupSeedsForRand(lSeed);
        long[] nnz = LibMatrixDatagen.computeNNZperBlock(rows, cols, rowsInBlock, colsInBlock, sparsity);
        double hdfsBlkSize = InfrastructureAnalyzer.getHDFSBlockSize();
        long numBlocks = nnz.length;
        long numColBlocks = (long) Math.ceil((double) cols / (double) colsInBlock);

        //a) in-memory seed dataset construction
        if (numBlocks < INMEMORY_NUMBLOCKS_THRESHOLD) {
            ArrayList<Tuple3<MatrixIndexes, Long, Long>> seeds = new ArrayList<Tuple3<MatrixIndexes, Long, Long>>();
            double partSize = 0;
            for (long i = 0; i < numBlocks; i++) {
                long r = 1 + i / numColBlocks;
                long c = 1 + i % numColBlocks;
                MatrixIndexes indx = new MatrixIndexes(r, c);
                Long seedForBlock = bigrand.nextLong();
                seeds.add(new Tuple3<MatrixIndexes, Long, Long>(indx, seedForBlock, nnz[(int) i]));
                partSize += nnz[(int) i] * 8 + 16;
            }
            //for load balancing: degree of parallelism such that ~128MB per partition
            int numPartitions = (int) Math.max(Math.min(partSize / hdfsBlkSize, numBlocks), 1);

            //create seeds dataset
            seedsData = flec.getFlinkContext().fromCollection(seeds).setParallelism(numPartitions);
        }
        //b) file-based seed rdd construction (for robustness wrt large number of blocks)
        else {
            String path = LibMatrixDatagen.generateUniqueSeedPath(dir);
            double partSize = 0;

            try {
                FileSystem fs = FileSystem.get(ConfigurationManager.getCachedJobConf());
                FSDataOutputStream fsOut = fs.create(new Path(path));
                PrintWriter pw = new PrintWriter(fsOut);
                StringBuilder sb = new StringBuilder();
                for (long i = 0; i < numBlocks; i++) {
                    sb.append(1 + i / numColBlocks);
                    sb.append(',');
                    sb.append(1 + i % numColBlocks);
                    sb.append(',');
                    sb.append(bigrand.nextLong());
                    sb.append(',');
                    sb.append(nnz[(int) i]);
                    pw.println(sb.toString());
                    sb.setLength(0);
                    partSize += nnz[(int) i] * 8 + 16;
                }
                pw.close();
                fsOut.close();
            } catch (IOException ex) {
                throw new DMLRuntimeException(ex);
            }

            //for load balancing: degree of parallelism such that ~128MB per partition
            int numPartitions = (int) Math.max(Math.min(partSize / hdfsBlkSize, numBlocks), 1);

            //create seeds dataset
            seedsData = flec.getFlinkContext().readCsvFile(path).types(Long.class, Long.class, Long.class,
                    Long.class).map(
                    new MapFunction<Tuple4<Long, Long, Long, Long>, Tuple3<MatrixIndexes, Long, Long>>() {
                        @Override
                        public Tuple3<MatrixIndexes, Long, Long> map(
                                Tuple4<Long, Long, Long, Long> value) throws Exception {
                            return new Tuple3<MatrixIndexes, Long, Long>(new MatrixIndexes(value.f0, value.f1),
                                    value.f2, value.f3);
                        }
                    });
        }

        // step 4: execute rand instruction over seed input
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out = seedsData.map(
                new MapFunction<Tuple3<MatrixIndexes, Long, Long>, Tuple2<MatrixIndexes, MatrixBlock>>() {
                    @Override
                    public Tuple2<MatrixIndexes, MatrixBlock> map(
                            Tuple3<MatrixIndexes, Long, Long> value) throws Exception {
                        //compute local block size:
                        MatrixIndexes ix = value.f0;
                        long blockRowIndex = ix.getRowIndex();
                        long blockColIndex = ix.getColumnIndex();
                        int lrlen = UtilFunctions.computeBlockSize(rows, blockRowIndex, rowsInBlock);
                        int lclen = UtilFunctions.computeBlockSize(cols, blockColIndex, colsInBlock);

                        long seed = value.f1;
                        long blockNNZ = value.f2;

                        MatrixBlock blk = new MatrixBlock();

                        RandomMatrixGenerator rgen = LibMatrixDatagen.createRandomMatrixGenerator(
                                pdf, lrlen, lclen, lrlen, lclen,
                                sparsity, minValue, maxValue, pdfParams);

                        blk.randOperationsInPlace(rgen, new long[]{blockNNZ}, null, seed);

                        return new Tuple2<MatrixIndexes, MatrixBlock>(value.f0, blk);
                    }
                });

        //step 5: output handling
        MatrixCharacteristics mcOut = flec.getMatrixCharacteristics(output.getName());
        if (!mcOut.dimsKnown(true)) {
            //note: we cannot compute the nnz from sparsity because this would not reflect the
            //actual number of non-zeros, except for extreme values of sparsity equals 0 or 1.
            long lnnz = (sparsity == 0 || sparsity == 1) ? (long) (sparsity * rows * cols) : -1;
            mcOut.set(rows, cols, rowsInBlock, colsInBlock, lnnz);
        }
        flec.setDataSetHandleForVariable(output.getName(), out);
    }

    /**
     * This will check if there is sufficient memory locally.
     *
     * @return
     */
    private boolean isMemAvail(long lRows, long lCols, double sparsity, double min, double max) {
        double size = (min == 0 && max == 0) ? OptimizerUtils.estimateSizeEmptyBlock(rows, cols) :
                OptimizerUtils.estimateSizeExactSparsity(rows, cols, sparsity);

        return (OptimizerUtils.isValidCPDimensions(rows, cols)
                && OptimizerUtils.isValidCPMatrixSize(rows, cols, sparsity)
                && size < OptimizerUtils.getLocalMemBudget());
    }
}
