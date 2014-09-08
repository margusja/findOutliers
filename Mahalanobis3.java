package com.nortal.wero;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.MahalanobisDistanceMeasure;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;

public class Mahalanobis3 {

	private static final Logger log = Logger.getLogger(Mahalanobis3.class.getName());

	public static class myMapper10 extends Mapper<Object, Text, Text, IntWritable>
	{
		
		private static final Logger log2 = Logger.getLogger(myMapper10.class.getName());
		private static Matrix matrix = new SparseMatrix(3, 3);
		private static double outlierLimit = 5;
		private Text word = new Text();
		private int count = 0;
		
		
		
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	    {
	    	
	    	log2.info("start map demo...");
	    	// create matrix
	    	double[][] d = { { 2.0, 2.0, 2.0 }, { 2.0, 2.0, 2.0 }, { 2.0, 2.0, 2.0 } };
	    	
	    	Vector v1 = new RandomAccessSparseVector(3);
			v1.assign(d[0]);
			Vector v2 = new RandomAccessSparseVector(3);
			v2.assign(d[1]);
			//System.out.println("Vector 2 size: "+ v2.size());
			Vector v4 = new RandomAccessSparseVector(3);
			v4.assign(d[2]);

			matrix.assignRow(0, v1);
			matrix.assignRow(1, v2);
			matrix.assignRow(2, v4);
	    	
	    	MahalanobisDistanceMeasure dmM = new MahalanobisDistanceMeasure();
			dmM.setInverseCovarianceMatrix(matrix);
			
			// Split input map input value
			String[] linePieses = value.toString().split(",");
			
			double d1 = Double.parseDouble(linePieses[0]);
			double d2 = Double.parseDouble(linePieses[1]);
			double d3 = Double.parseDouble(linePieses[2]);
			double[] dd = {d1, d2, d3};
			
			RandomAccessSparseVector v3 = new RandomAccessSparseVector(3);
			v3.assign(dd);
			
			double distance1 = dmM.distance(v2, v3);
			
			word.set(++count +" " +value.toString() + " " +distance1);
			context.write(word, new IntWritable(1));
			
	    }
	}
	
	public static class myReducer10 extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values,
                Context context
                ) throws IOException, InterruptedException {

			result.set(1);
			context.write(key, result);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		log.info("start demo with 3 parameters...");
		
		Configuration conf = getConf();
		
	    Job job = new Job(conf, "Outliers detection 3 parameters");
	    job.setJarByClass(Mahalanobis3.class);
	    job.setMapperClass(myMapper10.class);
	    job.setCombinerClass(myReducer10.class);
	    job.setReducerClass(myReducer10.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    HadoopUtil.delete(conf, new Path("output3"));
	    
	    FileInputFormat.addInputPath(job, new Path("input3"));
	    FileOutputFormat.setOutputPath(job,
	    	      new Path("output3"));
	    
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	private static Configuration getConf() {
		//System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = new Configuration();
		//conf.set("dfs.blocksize", "1048576");
		//conf.set("yarn.scheduler.maximum-allocation-mb", "1024");
		
		
		//conf.set("fs.default.name", "hdfs://vm38.dbweb.ee:8020");
		//conf.set("yarn.resourcemanager.address","vm38.dbweb.ee:8032");
		
		return conf;
	}

}
