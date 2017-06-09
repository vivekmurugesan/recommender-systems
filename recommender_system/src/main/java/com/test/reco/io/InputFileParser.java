package com.test.reco.io;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.test.reco.model.Rating;


/**
 * 
 * @author vivek
 *
 */
public class InputFileParser {

	public JavaRDD<Rating> parseFile(String fileUri, JavaSparkContext sc){
		JavaRDD<String> input = sc.textFile(fileUri);
		JavaRDD<Rating> ratingRdd = input.map(x -> {
			String[] tokens = x.split("\t");
			int userId = Integer.parseInt(tokens[0]);
			int itemId = Integer.parseInt(tokens[1]);
			float ratingVal = Float.parseFloat(tokens[2]);
			long timestamp = Long.parseLong(tokens[3]);
			
			return new Rating(userId, itemId, ratingVal,timestamp);
		});
		
		return ratingRdd;
	}
}
