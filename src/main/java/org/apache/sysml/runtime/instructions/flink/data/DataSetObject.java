package org.apache.sysml.runtime.instructions.flink.data;

import org.apache.flink.api.java.DataSet;
import org.apache.sysml.runtime.instructions.spark.data.LineageObject;

public class DataSetObject extends LineageObject {

    private DataSet<?> _dsHandle = null;

    //meta data on origin of given rdd handle
    private boolean _checkpointed = false; //created via checkpoint instruction
    private boolean _hdfsfile = false;     //created from hdfs file
    private String  _hdfsFname = null;     //hdfs filename, if created from hdfs.

    public DataSetObject (DataSet<?> dsvar, String varName) {
        _dsHandle = dsvar;
        _varName  = varName;
    }

    public DataSet<?> getDataSet() {
        return _dsHandle;
    }

    public boolean isCheckpointed() {
        return _checkpointed;
    }

    public void setCheckpointed(boolean checkpointed) {
        this._checkpointed = checkpointed;
    }

    public boolean isHDFSFile() {
        return _hdfsfile;
    }

    public void setHDFSFile(boolean hdfsfile) {
        this._hdfsfile = hdfsfile;
    }

    public String getHDFSFilename() {
        return _hdfsFname;
    }

    public void setHDFSFilename(String fname) {
        this._hdfsFname = fname;
    }
}
