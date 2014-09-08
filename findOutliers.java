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


public class findOutliers {

	private static final Logger log = Logger.getLogger(findOutliers.class.getName());
	
	public static class myMapper extends Mapper<Object, Text, Text, IntWritable>
	{
		
		private static final Logger log2 = Logger.getLogger(myMapper.class.getName());
		private static Matrix matrix = new SparseMatrix(2, 2);
		private static double outlierLimit = 5;
		private Text word = new Text();
		private int count = 0;
		
		
		
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	    {
	    	
	    	log2.info("start map demo...");
	    	// create matrix
	    	double[][] d = { { 1.0, 2.0 }, { 2.0, 4.0 }};
	    	
	    	Vector v1 = new RandomAccessSparseVector(2);
			v1.assign(d[0]);
			Vector v2 = new RandomAccessSparseVector(2);
			v2.assign(d[1]);
	    	
			matrix.assignRow(0, v1);
			matrix.assignRow(1, v2);
	    	
	    	MahalanobisDistanceMeasure dmM = new MahalanobisDistanceMeasure();
			dmM.setInverseCovarianceMatrix(matrix);
			
			// Split input map input value
			String[] linePieses = value.toString().split(",");
			
			double d1 = Double.parseDouble(linePieses[0]);
			double d2 = Double.parseDouble(linePieses[1]);
			double[] dd = {d1, d2};
			
			RandomAccessSparseVector v3 = new RandomAccessSparseVector(2);
			v3.assign(dd);
			
			double distance1 = dmM.distance(v2, v3);
			
			word.set(++count +" " +value.toString() + " " +distance1);
			context.write(word, new IntWritable(1));
			
	    }
	}
		
	
	public static class myReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
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
		
		log.info("start demo with 2 parameters...");
		
		Configuration conf = getConf();
		
	    Job job = new Job(conf, "Outliers detection 2 parameters");
	    job.setJarByClass(findOutliers.class);
	    job.setMapperClass(myMapper.class);
	    job.setCombinerClass(myReducer.class);
	    job.setReducerClass(myReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    HadoopUtil.delete(conf, new Path("output"));
	    
	    FileInputFormat.addInputPath(job, new Path("input"));
	    FileOutputFormat.setOutputPath(job,
	    	      new Path("output"));
	    
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	private static Configuration getConf() {
		//System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = new Configuration();
		//conf.set("mapreduce.map.cpu.vcores", "2");
		//conf.set("fs.default.name", "hdfs://vm38.dbweb.ee:8020");
		//conf.set("yarn.resourcemanager.address","vm38.dbweb.ee:8032");
		
		return conf;
	}

}
