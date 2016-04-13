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

package org.apache.sysml.runtime.instructions.flink.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;


public class IOUtils {

    public static void saveAsHadoopFile(DataSet<?> data, String path, Class outputKeyClass, Class outputValueClass,
                                        Class outputFormatClass) {

        Class<?> clazz = outputFormatClass;
        Constructor<?> ctor = null;
        FileOutputFormat outFormat = null;
        try {
            ctor = clazz.getConstructor();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        try {
            outFormat = (FileOutputFormat) ctor.newInstance(new FileOutputFormat[]{});
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        JobConf job = new JobConf();
        HadoopOutputFormat hadoopOutputFormat = new HadoopOutputFormat(outFormat, job);
        FileOutputFormat.setOutputPath(job, new Path(path));
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
        data.output(hadoopOutputFormat);
    }

    public static DataSet<?> hadoopFile(ExecutionEnvironment env, String path, Class inputFormatClass,
                                        Class inputKeyClass, Class inputValueClass) {

        Class<?> clazz = inputFormatClass;
        Constructor<?> ctor = null;
        FileInputFormat inFormat = null;
        try {
            ctor = clazz.getConstructor();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        try {
            inFormat = (FileInputFormat) ctor.newInstance(new FileInputFormat[]{});
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        JobConf job = new JobConf();
        HadoopInputFormat hadoopInputFormat = new HadoopInputFormat(inFormat, inputKeyClass, inputValueClass, job);
        FileInputFormat.addInputPath(job, new Path(path));

        DataSet<?> data = env.createInput(hadoopInputFormat);

        return data;
    }
}
