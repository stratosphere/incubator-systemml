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
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.mesos.protobuf.ByteString;
import org.apache.sysml.runtime.DMLRuntimeException;

import java.lang.reflect.Constructor;


public class IOUtils {

	@SuppressWarnings("unchecked")
	public static <K, V> void saveAsHadoopFile(
			DataSet<Tuple2<K, V>> data,
			String path,
			Class outputFormatClass,
			Class outputKeyClass,
			Class outputValueClass) throws DMLRuntimeException {

		OutputFormat outputFormat = null;
		try {
			Constructor<? extends OutputFormat> e = outputFormatClass.getDeclaredConstructor(new Class[0]);
			e.setAccessible(true);
			outputFormat = e.newInstance(new Object[0]);
		} catch (ReflectiveOperationException e) {
			throw new DMLRuntimeException("Could not instantiate output format", e);
		}

		JobConf job = new JobConf();
		FileOutputFormat.setOutputPath(job, new Path(path));
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);

		HadoopOutputFormat<K, V> hadoopOutputFormat = new HadoopOutputFormat<K, V>(outputFormat, job);
		data.output(hadoopOutputFormat);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> DataSource<Tuple2<K, V>> hadoopFile(
			ExecutionEnvironment env,
			String path,
			Class<? extends InputFormat> inputFormatClass,
			Class<K> inputKeyClass,
			Class<V> inputValueClass) throws DMLRuntimeException {

		InputFormat inputFormat = null;
		try {
			Constructor<? extends InputFormat> e = inputFormatClass.getDeclaredConstructor(new Class[0]);
			e.setAccessible(true);
			inputFormat = e.newInstance(new Object[0]);
		} catch (ReflectiveOperationException e) {
			throw new DMLRuntimeException("Could not instantiate input format", e);
		}

		JobConf job = new JobConf();
		job.setInputFormat(inputFormatClass);
		FileInputFormat.addInputPath(job, new Path(path));

		HadoopInputFormat<K, V> hadoopInputFormat = new HadoopInputFormat<K, V>(inputFormat, inputKeyClass,
				inputValueClass, job);

		return env.createInput(hadoopInputFormat);
	}
}
