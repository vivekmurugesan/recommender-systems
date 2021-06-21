package org.reco.sys.content.similarity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ContentSimilarityGenerator {
	
	private String outputDir;
	private String ratingFilePath;
	private String moviesFilePath;
	
	private List<String> allGenreList;

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Content Similarity Generator");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		ContentSimilarityGenerator simGenerator = new ContentSimilarityGenerator(args[0],
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

		/**
		 * Computing jaccard similarity
		 * sim = m11 / (m10 + m11 + m01)
		 */
		JavaPairRDD<Tuple2<Integer, Integer>, Double> jaccardSimRdd = 
				computeJaccardSimilarities(cartProd);

		
		// Computing euclidean similarity
		JavaPairRDD<Tuple2<Integer, Integer>, Double> euclideanSimRdd = 
				computeEuclideanSimilarities(cartProd);
		
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> joined = 
				jaccardSimRdd.join(euclideanSimRdd);

		System.out.println("Number of non-zero sim scores generated:" + 
				jaccardSimRdd.count());

		joined.map(x -> x._1._1 +", " + x._1._2 + ", " + x._2._1 + ", " + x._2._2 )
		.saveAsTextFile(outputDir+"/Similarities");
	}
	
	public JavaPairRDD<Tuple2<Integer, Integer>, Double>
	computeEuclideanSimilarities(JavaPairRDD<Tuple2<Integer, Movie>, Tuple2<Integer, Movie>> 
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
	
	public JavaPairRDD<Tuple2<Integer, Integer>, Double>
		computeJaccardSimilarities(JavaPairRDD<Tuple2<Integer, Movie>, Tuple2<Integer, Movie>> 
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
	
	private void createAllGenreList(JavaSparkContext sc, JavaPairRDD<Integer, Movie> moviesRdd) {
		this.allGenreList.addAll(moviesRdd.flatMapToPair(x -> {
			int movieId = x._1;
			List<String> genres = x._2.getGenres();
			List<Tuple2<Integer, String>> tuples = new ArrayList<>();
			for(String g : genres) {
				tuples.add(new Tuple2<>(movieId, g.trim()));
			}
			
			return tuples.iterator();
		}).map(x -> x._2).filter(x -> !x.contains(Movie.EXCLUDE)).distinct().collect());
	}

}
