package org.apache.sysml.runtime.instructions.flink.utils;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.nio.charset.Charset;

public class RowIndexedInputFormat extends DelimitedInputFormat<Tuple2<Integer, String>> {


    private static final long serialVersionUID = 1L;
    private String charsetName = "UTF-8";

    private int splitNumber;

    public void open(FileInputSplit split) throws IOException {
        this.splitNumber = split.getSplitNumber();
        super.open(split);
    }

    public Tuple2<Integer, String> readRecord(Tuple2<Integer, String> reuseable, byte[] bytes, int offset, int numBytes) throws IOException {
        if(this.getDelimiter() != null && this.getDelimiter().length == 1 && this.getDelimiter()[0] == 10 && offset + numBytes >= 1 && bytes[offset + numBytes - 1] == 13) {
            --numBytes;
        }

        return new Tuple2<Integer, String>(splitNumber, new String(bytes, offset, numBytes, this.charsetName));
    }


    public String getCharsetName() {
        return this.charsetName;
    }

    public void setCharsetName(String charsetName) {
        if(charsetName == null) {
            throw new IllegalArgumentException("Charset must not be null.");
        } else {
            this.charsetName = charsetName;
        }
    }

    public void configure(Configuration parameters) {
        super.configure(parameters);
        if(this.charsetName == null || !Charset.isSupported(this.charsetName)) {
            throw new RuntimeException("Unsupported charset: " + this.charsetName);
        }
    }

    public String toString() {
        return "TextInputFormat (" + this.getFilePath() + ") - " + this.charsetName;
    }
}
