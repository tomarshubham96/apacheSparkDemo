package com.apacheSpark.apacheSparkDemo;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import scala.Tuple2;

@SpringBootApplication
public class ApacheSparkDemoApplication {
	
	private static final Pattern SPACE = Pattern.compile(" ");

	  public static void main(String[] args) throws Exception {

		  SpringApplication.run(ApacheSparkDemoApplication.class, args);
		  
		  System.setProperty("hadoop.home.dir", "file:///C:/winutils/");
		  File workaround = new File(".");
		    System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
		    new File("./bin").mkdirs();
		    new File("./bin/winutils.exe").createNewFile();
//	    if (args.length < 1) {
//	      System.err.println("Usage: JavaWordCount <file>");
//	      System.exit(1);
//	    }
		  
		  Date start = new Date();

	    SparkSession spark = SparkSession
	      .builder()
	      .appName("ApacheSparkDemoApplication")
	      .config("spark.master", "local")
	      .getOrCreate();

	    JavaRDD<String> lines = spark.read().textFile("C:\\Users\\shubham\\Downloads\\doctoIndex\\*.txt").javaRDD();

	    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

	    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

	    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

//	    List<Tuple2<String, Integer>> output = counts.collect();
//	    for (Tuple2<?,?> tuple : output) {
//	      System.out.println(tuple._1() + ": " + tuple._2());
//	    }
	    
	    counts.saveAsTextFile("D:\\ETL-POC-Resources\\output\\countAgain");
	    
	     spark.stop();
	     Date end = new Date();
		  System.out.println((end.getTime() - start.getTime())/1000 + " --------*********total milliseconds");
	  }

}
