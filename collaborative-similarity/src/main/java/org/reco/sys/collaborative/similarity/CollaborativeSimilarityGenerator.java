package org.reco.sys.collaborative.similarity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

/**
 * Generates collaborative similarity scores between movies based on the rating
 * co-occurrences with the following mechanisms,
 * 1. Cosine Similarity
 * 2. Jaccard Similarity
 * 3. Euclidean distance based
 * 
 * Get the dataset from this location:
 * https://grouplens.org/datasets/movielens/10m/
 * 
 * and pass in ratings.dat file as the input for this code.
 * 
 * @author Vivek Murugesan
 *
 */
public class CollaborativeSimilarityGenerator {
	
	private String outputDir;
	private String ratingFilePath;
	private String moviesFilePath;
	
	private List<String> allGenreList;

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName(
				"Content Similarity Generator");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		CollaborativeSimilarityGenerator simGenerator = 
				new CollaborativeSimilarityGenerator(args[0],
				args[1], args[2]);
		simGenerator.generateSimilarities(sc);
	}
	
	public CollaborativeSimilarityGenerator(String outputDir,
			String ratingsFilePath, String moviesFilePath) {
		this.outputDir = outputDir;
		this.ratingFilePath = ratingsFilePath;
		this.moviesFilePath = moviesFilePath;
		this.allGenreList = new ArrayList<>();
	}
	
	public void generateSimilarities(JavaSparkContext sc) {

		JavaPairRDD<Integer, Integer> ratingsRdd = processRatingsData(sc);

		
		JavaPairRDD<Tuple2<Integer, Integer>, Integer>
		cooccurrencesCountRdd = 
				computeCooccurrenceCount(ratingsRdd).cache();
		
		Map<Integer, Integer> movieRatingsFreq =
				computeMovieRatingFreq(ratingsRdd);
		
		movieRatingsFreq = sc.broadcast(movieRatingsFreq).getValue();
		

		// Computing jaccard similarity
		JavaPairRDD<Tuple2<Integer, Integer>, Double> jaccardSimRdd = 
				computeJaccardSimilarities(cooccurrencesCountRdd,
						movieRatingsFreq);


		// Computing euclidean similarity
		JavaPairRDD<Tuple2<Integer, Integer>, Double> euclideanSimRdd = 
				computeEuclideanSimilarities(cooccurrencesCountRdd,
						movieRatingsFreq).cache();

		// Computing cosine similarity
		JavaPairRDD<Tuple2<Integer, Integer>, Double> cosineSimRdd = 
				computeEuclideanSimilarities(cooccurrencesCountRdd,
						movieRatingsFreq).cache();

		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> joined = 
				jaccardSimRdd.join(euclideanSimRdd).cache();

		JavaPairRDD<Tuple2<Integer, Integer>, Tuple3<Double, Double, Double>> 
		jacEucCosineRdd =
		joined.join(cosineSimRdd).mapToPair(x ->
		new Tuple2<>(x._1, new Tuple3<>(x._2._1._1, x._2._1._1, x._2._2)));

		System.out.println("Number of non-zero sim scores generated:" + 
				jacEucCosineRdd.count());

		jacEucCosineRdd.map(x -> x._1._1 +", " + x._1._2 + ", " 
				+ x._2._1() + ", " + x._2._2() + ", " + x._2._3() )
		.saveAsTextFile(outputDir+"/Similarities");
	}
	
	private JavaPairRDD<Tuple2<Integer, Integer>, Integer> 
	computeCooccurrenceCount(JavaPairRDD<Integer, Integer> ratingsRdd){

		JavaPairRDD<Integer, Integer> cooccurrencesRdd = 
				ratingsRdd.join(ratingsRdd).mapToPair(x -> x._2);

		JavaPairRDD<Tuple2<Integer, Integer>, Integer> 
		cooccurrencesCountRdd = 
		cooccurrencesRdd.mapToPair(x -> new Tuple2<>(x, 1))
			.reduceByKey((a,b) -> a+b);

		return cooccurrencesCountRdd;
	}
	
	private Map<Integer, Integer> computeMovieRatingFreq(
			JavaPairRDD<Integer, Integer> ratingsRdd){
		
		JavaPairRDD<Integer, Integer> moviesFreqRdd = 
		ratingsRdd.map(x -> x._2).mapToPair(x -> new Tuple2<>(x, 1))
			.reduceByKey((a,b) -> a+b);
		
		return moviesFreqRdd.collectAsMap();
	}
	
	/**
	 * Cosine similarity between two vectors A & B is computed based 
	 * on the formula:
	 *   	A.B / (||A|| x ||B||)
	 * 1. The numerator in case of binary data can be simplified as the size
	 * (cardinality) of the intersection |A n B|
	 * 2. The denominator can be simplified as Sqrt(|A|) * Sqrt(|B|), where
	 * |A| and |B| are the size of A and B respectively.
	 * @param cartProd
	 * @param sc
	 * @return
	 */
	public JavaPairRDD<Tuple2<Integer, Integer>, Double>
	computeCosineSimilarities(
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> cooccurrencesCountRdd,
		Map<Integer, Integer> movieRatingFreq){
		JavaPairRDD<Tuple2<Integer, Integer>, Double> cosineSimRdd = 
				cooccurrencesCountRdd.mapToPair(x -> {
					int movie1Id = x._1._1;
					int movie2Id = x._1._2;

					int m11 = x._2;
					int m1Freq = movieRatingFreq.get(movie1Id);
					int m2Freq = movieRatingFreq.get(movie2Id);
					
					double sim = m11 * 1.0 / 
					(Math.sqrt(m1Freq) * 
					 Math.sqrt(m2Freq));

					
					return new Tuple2<>(new Tuple2<>(movie1Id, movie2Id), sim);
				});
			return cosineSimRdd;
	}
	
	/**
	 * Euclidean distance is the value of square root of squared sum of the 
	 * differences in each axes or dimension.
	 * 	Sqrt(Sum((Ai-Bi)^2))
	 * 
	 * In this binary data case it can be simplified as,
	 * 	Sqrt(m10 + m01)
	 * as the difference in rest of the dimensions will be zero.
	 * @param cartProd
	 * @return
	 */
	public JavaPairRDD<Tuple2<Integer, Integer>, Double>
	computeEuclideanSimilarities(
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> cooccurrencesCountRdd,
		Map<Integer, Integer> movieRatingFreq){
		JavaPairRDD<Tuple2<Integer, Integer>, Double> euclideanSimRdd = 
				cooccurrencesCountRdd.mapToPair(x -> {
					int movie1Id = x._1._1;
					int movie2Id = x._1._2;

					int m11 = x._2;
					int m10 = movieRatingFreq.get(movie1Id) - m11;
					int m01 = movieRatingFreq.get(movie2Id) - m11;

					double euclideanDist = Math.sqrt(m10 + m01);
					double sim = 1.0/(1.0 + euclideanDist);

					return new Tuple2<>(new Tuple2<>(movie1Id, movie2Id), sim);
				});
		return euclideanSimRdd;
	}
	
	/**
	 * Jaccard similarity between 2 vectors A and B is computed based on a 
	 * simple formula: m11 / (m10 + m11 + m01)
	 * This equation is specifically designed for binary data. Where,
	 * m11 - number of instances where both the vectors has a value of 1
	 * m10 - number of instances where A has a value 1 and B has a value 0
	 * m01 - number of instances where A has a value 0 and B has a value 1
	 * @param cartProd
	 * @return
	 */
	public JavaPairRDD<Tuple2<Integer, Integer>, Double>
		computeJaccardSimilarities(
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> cooccurrencesCountRdd,
		Map<Integer, Integer> movieRatingFreq) {
		
		
		JavaPairRDD<Tuple2<Integer, Integer>, Double> jaccardSimRdd = 
		cooccurrencesCountRdd.mapToPair(x -> {
			int movie1Id = x._1._1;
			int movie2Id = x._1._2;
			
			int m11 = x._2;
			int m10 = movieRatingFreq.get(movie1Id) - m11;
			int m01 = movieRatingFreq.get(movie2Id) - m11;
			
			double jaccardSim = (m11 * 1.0) / (m10 + m11 + m01);

			return new Tuple2<>(new Tuple2<>(movie1Id, movie2Id), jaccardSim);
		});
		
		/* JavaPairRDD<Tuple2<Integer, Integer>, Double> jaccardSimFilteredRdd = 
				jaccardSimRdd.filter(x -> x._2 > 0.0); */
		
		return jaccardSimRdd;
	}
	
	public JavaPairRDD<Integer, Integer> processRatingsData(
			JavaSparkContext sc) {
		// Loading Movies data --> movie_id::title::tag1|tag2...
		// Ratings data in the form --> UserID::MovieID::Rating::Timestamp

		JavaRDD<String> ratingsFile = sc.textFile(ratingFilePath).cache();

		JavaPairRDD<Integer, Integer> ratingsRdd = 
				ratingsFile.mapToPair(x -> {
					String[] tokens = x.split("::");
					int userId = Integer.parseInt(tokens[0]);
					int movieId = Integer.parseInt(tokens[1]);
					
					return new Tuple2<>(userId, movieId);

				});
		
		return ratingsRdd;

	}	
	

}
