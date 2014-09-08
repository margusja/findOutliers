package com.nortal.wero;

import java.io.File;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nortal.wero.Utils;

public class KMeansClustering {

	private static final Logger LOG = LoggerFactory.getLogger(KMeansClustering.class);
    private static final String POINTS_PATH = "points";
    private static final String CLUSTERS_PATH = "clusters";
    private static final String OUTPUT_PATH = "clusteroutput";
    private int vectorLenght;
		
    private final int numberOfClusters = 2;
	
    private void start() throws Exception {
     
            final Configuration configuration = new Configuration();
            
            // Create input directories for data
            final File pointsDir = new File(POINTS_PATH);
            if (!pointsDir.exists()) {
                pointsDir.mkdir();
            }
            
            FileSystem fs = FileSystem.get(configuration);
            //List<Vector> vectors = getPoints(points);
            
            Utils util = new Utils();
            
            List<Vector> vectors = util.generateVectors(100, vectorLenght, 10);
            
            Path path = new Path("clusters/part-00000");
            SequenceFile.Writer writer = new SequenceFile.Writer(fs, configuration, path, Text.class, Kluster.class);
            for (int i = 0; i < numberOfClusters; i++) {
                Vector vec = vectors.get(i);
                
                Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
                System.out.println(cluster.getIdentifier());
                writer.append(new Text(cluster.getIdentifier()), cluster);
                
              }
            
            writer.close();
     
            // Run K-means algorithm
            final Path inputPath = new Path(POINTS_PATH);
            final Path clustersPath = new Path(CLUSTERS_PATH);
            final Path outputPath = new Path(OUTPUT_PATH);
            HadoopUtil.delete(configuration, outputPath);
            
            KMeansDriver.run(configuration, inputPath, clustersPath, outputPath, 0.001, 10, true, 0, false);
     
            System.out.println("Clusters are ready");
            
           
/*            SequenceFile.Reader reader
            = new SequenceFile.Reader(fs,
                new Path("clusteroutput/" + Kluster.CLUSTERED_POINTS_DIR
                    + "/part-m-00000"), configuration);
            
            IntWritable key = new IntWritable();
            WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable(); 
            
            //System.out.println(reader.toString());
            while (reader.next(key, value)) {
                System.out.println( value.toString() + " belongs to cluster " + key.toString());
            }
            reader.close();*/
            
            System.out.println("The End");
        }
    
    	    	
	public static void main(String[] args) {
		
		final KMeansClustering application = new KMeansClustering();
		
		application.vectorLenght = Integer.parseInt(args[0]);
		 
        try {
            application.start();
        }
        catch (final Exception e) {
            LOG.error("KMeansClustering failed", e);
        }
		
	}
  
}
