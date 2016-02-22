package org.apache.sysml.runtime.instructions.flink.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.spark.data.SerLongWritable;
import org.apache.sysml.runtime.instructions.spark.data.SerText;
import org.apache.sysml.runtime.io.IOUtilFunctions;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.util.UtilFunctions;

import java.util.Iterator;


public class DatasetConverterUtils {

    public static DataSet<Tuple2<MatrixIndexes, MatrixBlock>> csvToBinaryBlock(ExecutionEnvironment env,
                                                                               DataSet<String> input,
                                                                               MatrixCharacteristics mcOut,
                                                                               boolean hasHeader,
                                                                               String delim,
                                                                               boolean fill,
                                                                               double fillValue) throws DMLRuntimeException {
        //convert string rdd to serializable longwritable/text
        DataSet<Tuple2<LongWritable, Text>> prepinput =
                input.map(new StringToSerTextFunction());

        //convert to binary block
        return csvToBinaryBlock2(env, prepinput, mcOut, hasHeader, delim, fill, fillValue);
    }

    public static DataSet<Tuple2<MatrixIndexes, MatrixBlock>> csvToBinaryBlock2(ExecutionEnvironment env,
                                                                               DataSet<Tuple2<LongWritable, Text>> input,
                                                                               MatrixCharacteristics mcOut,
                                                                               boolean hasHeader,
                                                                               String delim,
                                                                               boolean fill,
                                                                               double fillValue) throws DMLRuntimeException {
        //determine unknown dimensions and sparsity if required
//        if( !mcOut.dimsKnown(true) ) {
//            try {
//                long rlen = input.count() - (hasHeader ? 1 : 0);
//                long clen = input.first(1).collect().get(0).f1.toString().split(delim).length;
//                mcOut.set(rlen, clen, mcOut.getRowsPerBlock(), mcOut.getColsPerBlock(), 0L);
//                //TODO add nnz estimate
//            } catch (Exception e) {
//                throw new DMLRuntimeException("Could not get metadata for input dataset: ", e);
//            }
//        }

        //prepare csv w/ row indexes (sorted by filenames)
        DataSet<Tuple2<Long,Text>> prepinput = DataSetUtils.zipWithIndex(input.map(new MapFunction<Tuple2<LongWritable, Text>, Text>() {
            @Override
            public Text map(Tuple2<LongWritable, Text> value) throws Exception {
                return value.f1;
            }
        }));

        //convert csv rdd to binary block rdd (w/ partial blocks)
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out = prepinput.mapPartition(new CSVToBinaryBlockFunction(mcOut, delim, fill, fillValue));

        //aggregate partial matrix blocks
        out = DatasetAggregateUtils.mergeByKey( out );

        return out;
    }

    /**
     * Convert String to serializable <Longwritable, Text>
     */
    private static class StringToSerTextFunction implements MapFunction<String, Tuple2<LongWritable, Text>> {

        private static final long serialVersionUID = 2286037080400222528L;

        @Override
        public Tuple2<LongWritable, Text> map(String value) throws Exception {
            SerLongWritable slarg = new SerLongWritable(1L);
            SerText starg = new SerText(value);
            return new Tuple2<LongWritable,Text>(slarg, starg);
        }
    }

    /**
     * This functions allows to map rdd partitions of csv rows into a set of partial binary blocks.
     *
     * NOTE: For this csv to binary block function, we need to hold all output blocks per partition
     * in-memory. Hence, we keep state of all column blocks and aggregate row segments into these blocks.
     * In terms of memory consumption this is better than creating partial blocks of row segments.
     *
     */
    private static class CSVToBinaryBlockFunction implements MapPartitionFunction<Tuple2<Long,Text>, Tuple2<MatrixIndexes,MatrixBlock>>
    {
        private static final long serialVersionUID = -4948430402942717043L;

        private long _rlen = -1;
        private long _clen = -1;
        private int _brlen = -1;
        private int _bclen = -1;
        private String _delim = null;
        private boolean _fill = false;
        private double _fillValue = 0;

        public CSVToBinaryBlockFunction(MatrixCharacteristics mc, String delim, boolean fill, double fillValue)
        {
            _rlen = mc.getRows();
            _clen = mc.getCols();
            _brlen = mc.getRowsPerBlock();
            _bclen = mc.getColsPerBlock();
            _delim = delim;
            _fill = fill;
            _fillValue = fillValue;
        }

        // Creates new state of empty column blocks for current global row index.
        private void createBlocks(long rowix, int lrlen, MatrixIndexes[] ix, MatrixBlock[] mb)
        {
            //compute row block index and number of column blocks
            long rix = UtilFunctions.computeBlockIndex(rowix, _brlen);
            int ncblks = (int)Math.ceil((double)_clen/_bclen);

            //create all column blocks (assume dense since csv is dense text format)
            for( int cix=1; cix<=ncblks; cix++ ) {
                int lclen = (int)UtilFunctions.computeBlockSize(_clen, cix, _bclen);
                ix[cix-1] = new MatrixIndexes(rix, cix);
                mb[cix-1] = new MatrixBlock(lrlen, lclen, false);
            }
        }

        // Flushes current state of filled column blocks to output list.
        private void flushBlocksToList( MatrixIndexes[] ix, MatrixBlock[] mb, Collector<Tuple2<MatrixIndexes, MatrixBlock>> out )
                throws DMLRuntimeException
        {
            int len = ix.length;
            for( int i=0; i<len; i++ )
                if( mb[i] != null ) {
                    out.collect(new Tuple2<MatrixIndexes,MatrixBlock>(ix[i],mb[i]));
                    mb[i].examSparsity(); //ensure right representation
                }
        }

        @Override
        public void mapPartition(Iterable<Tuple2<Long, Text>> values, Collector<Tuple2<MatrixIndexes, MatrixBlock>> out) throws Exception {

            int ncblks = (int)Math.ceil((double)_clen/_bclen);
            MatrixIndexes[] ix = new MatrixIndexes[ncblks];
            MatrixBlock[] mb = new MatrixBlock[ncblks];
            Iterator<Tuple2<Long, Text>> arg0 = values.iterator();

            while( arg0.hasNext() )
            {
                Tuple2<Long,Text> tmp = arg0.next();
                String row = tmp.f1.toString();
                long rowix = tmp.f0 + 1;

                long rix = UtilFunctions.computeBlockIndex(rowix, _brlen);
                int pos = UtilFunctions.computeCellInBlock(rowix, _brlen);

                //create new blocks for entire row
                if( ix[0] == null || ix[0].getRowIndex() != rix ) {
                    if( ix[0] !=null )
                        flushBlocksToList(ix, mb, out);
                    long len = UtilFunctions.computeBlockSize(_rlen, rix, _brlen);
                    createBlocks(rowix, (int)len, ix, mb);
                }

                //process row data
                String[] parts = IOUtilFunctions.split(row, _delim);
                boolean emptyFound = false;
                for( int cix=1, pix=0; cix<=ncblks; cix++ )
                {
                    int lclen = (int)UtilFunctions.computeBlockSize(_clen, cix, _bclen);
                    for( int j=0; j<lclen; j++ ) {
                        String part = parts[pix++];
                        emptyFound |= part.isEmpty() && !_fill;
                        double val = (part.isEmpty() && _fill) ?
                                _fillValue : Double.parseDouble(part);
                        mb[cix-1].appendValue(pos, j, val);
                    }
                }

                //sanity check empty cells filled w/ values
                IOUtilFunctions.checkAndRaiseErrorCSVEmptyField(row, _fill, emptyFound);
            }

            //flush last blocks
            flushBlocksToList(ix, mb, out);
        }
    }
}
