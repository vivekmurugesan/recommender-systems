package org.reco.sys.content.similarity;

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

public class ContentSimilarityGenerator {
	
	private String outputDir;
	private String ratingFilePath;
	private String moviesFilePath;
	
	private List<String> allGenreList;

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName(
				"Content Similarity Generator");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		ContentSimilarityGenerator simGenerator = 
				new ContentSimilarityGenerator(args[0],
				args[1], args[2]);
		simGenerator.generateSimilarities(sc);
	}
	
	public ContentSimilarityGenerator(String outputDir,
			String ratingsFilePath, String moviesFilePath) {
		this.outputDir = outputDir;
		this.ratingFilePath = ratingsFilePath;
		this.moviesFilePath = moviesFilePath;
		this.allGenreList = new ArrayList<>();
	}
	
	public void generateSimilarities(JavaSparkContext sc) {

		JavaPairRDD<Integer, Movie> moviesRdd = processMovieData(sc);

		this.createAllGenreList(sc, moviesRdd);

		JavaPairRDD<Tuple2<Integer, Movie>, Tuple2<Integer, Movie>> cartProd 
		= moviesRdd.cartesian(moviesRdd);
		System.out.println("Number of cart prods:" + 
				cartProd.count());

		// Computing jaccard similarity
		JavaPairRDD<Tuple2<Integer, Integer>, Double> jaccardSimRdd = 
				computeJaccardSimilarities(cartProd);


		// Computing euclidean similarity
		JavaPairRDD<Tuple2<Integer, Integer>, Double> euclideanSimRdd = 
				computeEuclideanSimilarities(cartProd).cache();

		// Computing cosine similarity
		JavaPairRDD<Tuple2<Integer, Integer>, Double> cosineSimRdd = 
				computeEuclideanSimilarities(cartProd).cache();

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
	computeCosineSimilarities(JavaPairRDD<Tuple2<Integer, Movie>, 
			Tuple2<Integer, Movie>> 
	cartProd, JavaSparkContext sc){

		JavaPairRDD<Tuple2<Integer, Integer>, Double> cosineSimRdd =
				cartProd.mapToPair(x -> {
					int movie1Id = x._1._1;
					Movie movie1 = x._1._2;

					int movie2Id = x._2._1;
					Movie movie2 = x._2._2;

					Map<String, Integer> genreMap = new HashMap<>();
					int m11 = 0;

					List<String> m1Genres = movie1.getGenres();
					for(String g : m1Genres)
						genreMap.put(g, 1);

					// m11 captures the intersection between two vectors
					List<String> m2Genres = movie2.getGenres();
					for(String g : m2Genres) {
						if(m1Genres.contains(g))
							m11++;
					}

					double sim = m11 * 1.0 / 
							(Math.sqrt(movie1.getGenres().size()) * 
									Math.sqrt(movie2.getGenres().size()));

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
	computeEuclideanSimilarities(JavaPairRDD<Tuple2<Integer, Movie>, 
			Tuple2<Integer, Movie>> 
	cartProd){

		JavaPairRDD<Tuple2<Integer, Integer>, Double> euclideanSimRdd = 
				cartProd.mapToPair(x -> {
					int movie1Id = x._1._1;
					Movie movie1 = x._1._2;

					int movie2Id = x._2._1;
					Movie movie2 = x._2._2;

					Map<String, Integer> genreMap = new HashMap<>();
					int m11 = 0;
					int m10 = 0;
					int m01 = 0;

					List<String> m1Genres = movie1.getGenres();
					for(String g : m1Genres)
						genreMap.put(g, 1);

					List<String> m2Genres = movie2.getGenres();
					for(String g : m2Genres) {
						if(m1Genres.contains(g))
							m11++;
						else
							m01++;
					}

					m10 = genreMap.size() - (m11 + m01);

					double dist = Math.sqrt((m10 + m01) * 1.0);

					double sim = 1.0 / (1.0 + dist);

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
		computeJaccardSimilarities(JavaPairRDD<Tuple2<Integer, Movie>, 
				Tuple2<Integer, Movie>> 
		cartProd) {
		JavaPairRDD<Tuple2<Integer, Integer>, Double> jaccardSimRdd = 
				cartProd.mapToPair(x -> {

			int movie1Id = x._1._1;
			Movie movie1 = x._1._2;

			int movie2Id = x._2._1;
			Movie movie2 = x._2._2;

			Map<String, Integer> genreMap = new HashMap<>();
			int m11 = 0;
			int m10 = 0;
			int m01 = 0;

			List<String> m1Genres = movie1.getGenres();
			for(String g : m1Genres)
				genreMap.put(g, 1);

			List<String> m2Genres = movie2.getGenres();
			for(String g : m2Genres) {
				if(m1Genres.contains(g))
					m11++;
				else
					m01++;
			}

			m10 = genreMap.size() - (m11 + m01);

			double jaccardSim = (m11 * 1.0) / (m10 + m11 + m01);

			return new Tuple2<>(new Tuple2<>(movie1Id, movie2Id), jaccardSim);

		});
		
		/* JavaPairRDD<Tuple2<Integer, Integer>, Double> jaccardSimFilteredRdd = 
				jaccardSimRdd.filter(x -> x._2 > 0.0); */
		
		return jaccardSimRdd;
	}
	
	public JavaPairRDD<Integer, Movie> processMovieData(JavaSparkContext sc) {
		// Loading Movies data --> movie_id::title::tag1|tag2...

		JavaRDD<String> moviesFile = sc.textFile(moviesFilePath).cache();

		JavaPairRDD<Integer, Movie> moviesRdd = 
				moviesFile.mapToPair(x -> {
					String[] tokens = x.split("::");
					int movieId = Integer.parseInt(tokens[0]);
					Movie movie = new Movie(movieId, tokens[1]);
					String[] genres = tokens[2].split("\\|");
					List<String> genreList = new ArrayList<>();
					for(String t : genres)
						genreList.add(t);
					movie.setGenres(genreList);

					return new Tuple2<>(movieId, movie);

				});
		
		return moviesRdd;

	}
	
	private void createAllGenreList(JavaSparkContext sc, 
			JavaPairRDD<Integer, Movie> moviesRdd) {
		this.allGenreList.addAll(moviesRdd.flatMapToPair(x -> {
			int movieId = x._1;
			List<String> genres = x._2.getGenres();
			List<Tuple2<Integer, String>> tuples = new ArrayList<>();
			for(String g : genres) {
				tuples.add(new Tuple2<>(movieId, g.trim()));
			}

			return tuples.iterator();
		}).map(x -> x._2).filter(x -> !x.contains(Movie.EXCLUDE))
				.distinct().collect());
	}

}
