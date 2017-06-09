package com.test.reco.als;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

import com.test.reco.io.InputFileParser;
import com.test.reco.model.Rating;

/**
 * 
 * @author vivek
 *
 */
public class ALSRecoGenerator {
	
	private static final String FILE_URI = "src/main/resources/ml-100k/u.data";
	
	private String fileUri;
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Movie Lens choice generator")
				.set("spark.executor.memory", "64g")
				.set("spark.num.executors", "1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		ALSRecoGenerator recoGen;
		if(args.length <= 0)
			recoGen = new ALSRecoGenerator(FILE_URI);
		else
			recoGen = new ALSRecoGenerator(args[0]);
		recoGen.generateAndEvalChoices(sc);
	}
	
	public ALSRecoGenerator(String fileUri){
		this.fileUri = fileUri;
	}
	
	public void generateAndEvalChoices(JavaSparkContext sc){
		InputFileParser fileProc = new InputFileParser();
		JavaRDD<Rating> ratingRdd = fileProc.parseFile(fileUri, sc);
		SQLContext sqlContext = new SQLContext(sc);
		
		Dataset<Row> ratings = sqlContext.createDataFrame(ratingRdd, Rating.class);
		Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
		Dataset<Row> training = splits[0];
		Dataset<Row> test = splits[1];

		// Build the recommendation model using ALS on the training data
		ALS als = new ALS()
		  .setMaxIter(5)
		  .setRegParam(0.01)
		  .setUserCol("userId")
		  .setItemCol("itemId")
		  .setRatingCol("rating");
		ALSModel model = als.fit(training);

		// Evaluate the model by computing the RMSE on the test data
		Dataset<Row> predictions = model.transform(test);
		
		evalPredictions(predictions);
		
		//evalWithLibEval(predictions);
		//printPredictions(predictions);
		
	}
	
	public void printPredictions(Dataset<Row> predictions){
		System.out.println("predictions..");
		System.out.println(predictions);
		predictions.foreach(x ->{
			System.out.println(x.toString());
		});
	}
	
	public void evalPredictions(Dataset<Row> predictions){
		JavaRDD<Tuple2<Float, Float>> ratingVsPred =   
				predictions.toJavaRDD().map(x -> {
					int itemId = x.getInt(0);
					float rating = x.getFloat(1);
					float pred = x.getFloat(4);
					System.err.printf("rating:%f, pred:%f\n", rating, pred);
					return new Tuple2<>(rating, pred);
				});
		JavaRDD<Float> sqErrRdd = 
				ratingVsPred.map(x -> {
					float sqErr = (x._1 - x._2) * (x._1 - x._2);
					System.err.println("sq err:" + sqErr);
					return sqErr;
				});
		long countBef = sqErrRdd.count();
		System.out.println("Count before filtering:" + countBef);
		sqErrRdd = 
				sqErrRdd.filter(x -> !x.equals(Float.NaN));

		long count = sqErrRdd.count();
		System.out.println("Count after filtering:" + count);
		float sqErrSum = sqErrRdd.fold(0.0f, (x,y) -> x+y);
		double rmseLocal = Math.sqrt(sqErrSum/count);
		System.out.println("Root-mean-square error local code =" + rmseLocal);
		
		
		JavaRDD<Float> absErrRdd = 
				ratingVsPred.map(x -> {
					float absErr = Math.abs(x._1 - x._2);
					System.err.println("sq err:" + absErr);
					return absErr;
				});
		absErrRdd = 
				absErrRdd.filter(x -> !x.equals(Float.NaN));
		float absErrSum = absErrRdd.fold(0.0f, (x,y) -> x+y);
		float mae = absErrSum/count;
		System.out.println("Mean absolute error="+ mae);
	}
	
	public void evalWithLibEval(Dataset<Row> predictions){
		RegressionEvaluator evaluator = new RegressionEvaluator()
		  .setMetricName("rmse")
		  .setLabelCol("rating")
		  .setPredictionCol("prediction");
		Double rmse = evaluator.evaluate(predictions);
		System.out.println("Use spark version >= 2.2 if you see rmse NaN:");
		System.out.println("https://issues.apache.org/jira/browse/SPARK-14489");
		
		System.out.println("Root-mean-square error = " + rmse);
		
		evaluator = new RegressionEvaluator()
		  .setMetricName("mae")
		  .setLabelCol("rating")
		  .setPredictionCol("prediction");
		Double mae = evaluator.evaluate(predictions);
		System.out.println("Mean Absoulute error = " + mae);
	}

}
