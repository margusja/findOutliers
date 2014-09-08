package com.nortal.wero;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
	private Logger log = LoggerFactory.getLogger(Utils.class);
	private int minRand = 1;
	private static List<Vector> points = new ArrayList<Vector>();
	
	public List<Vector> generateVectors(int x, int y, int max)
	{
		
		Random rand = new Random();
		
		double[] fr = new double[y];
		
		for (int i = 0; i < x; i++) {
			
			for (int j = 0; j < y; j++) {
				fr[j] = rand.nextInt(max) + minRand;
			}
			
  			Vector vec = new RandomAccessSparseVector(y);
			vec.assign(fr);
			//System.out.println(vec.toString());
			points.add(vec);
			
		}
		return points;
	}
	
	private void writePointsToFile(List<Vector> points, String fileName, FileSystem fs, Configuration conf) throws IOException {
		Path path = new Path(fileName);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
				path, LongWritable.class, VectorWritable.class);
		long recNum = 0;
		VectorWritable vec = new VectorWritable();
		for (Vector point : points) {
			//System.out.println(point.toString());
			vec.set(point);
			writer.append(new LongWritable(recNum++), vec);
		}
		writer.close();
	}
	
	private void readClusters(FileSystem fs, Configuration conf, int lenght) throws IOException
	{
		//final Configuration conf = new Configuration();
		//FileSystem fs = FileSystem.get(conf);
		
        SequenceFile.Reader reader
        = new SequenceFile.Reader(fs,
            new Path("clusteroutput/" + Kluster.CLUSTERED_POINTS_DIR
                + "/part-m-00000"), conf);
        
        IntWritable key = new IntWritable();
        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable(); 
        
        //System.out.println(reader.toString());
        int i = 0;
        while (reader.next(key, value)) {
        	if (i > lenght)
        	{
        		break;
        		
        	}
            System.out.println( value.toString() + " belongs to cluster " + key.toString());
            i++;
        }
        reader.close();
	}
	
	protected void genCSV(int x, int y, int max, String fileName) throws IOException {
		Random rand = new Random();
		final Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(fileName);
		
		FSDataOutputStream out = fs.create(path);
		
		for (int i = 0; i < x; i++) {
			
			StringBuilder sb=new StringBuilder();
			for (int j = 0; j < y; j++) {
				if (j == 0)
					sb.append(String.valueOf(rand.nextInt(max) + minRand));
				else
					sb.append(", "+ String.valueOf(rand.nextInt(max) + minRand));
			}
			

			byte[] byt=sb.toString().getBytes();
			out.write(byt);
			out.write(System.getProperty("line.separator").getBytes());
		}
		out.close();
	}
	
	public static void main(String[] args) throws IOException {
		
		Utils util = new Utils();
		
		if (args.length < 0)
			System.out.println("Usage: 1) genCSV x y max filename 2) genVectors x y max filename");
		
		else if (args[0].contains("genCSV")) {
			util.genCSV(Integer.valueOf(args[1]), Integer.valueOf(args[2]), Integer.valueOf(args[3]), args[4]);
		}
		else if (args[0].contains("genVectors")) {
			util.generateVectors(Integer.valueOf(args[1]), Integer.valueOf(args[2]), Integer.valueOf(args[3]));
			
			final Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			util.writePointsToFile(points, args[4], fs, conf);
		}
		else if (args[0].contains("readClusters")) {
			
			final Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			util.readClusters(fs, conf, Integer.parseInt(args[1])); // args[1] how many rows we are going to display from the result
		}
		

		


	}

}
