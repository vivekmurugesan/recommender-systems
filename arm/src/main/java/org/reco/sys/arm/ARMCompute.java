package org.reco.sys.arm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple8;

public class ARMCompute implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String transFilePath;
	private String outputDir;
	private boolean applyFlatten;
	private boolean applyOnTestData;
	private static String FLATTEN_ARG_PREFIX = "flatten:";
	private static String APPLY_ON_TEST_ARG_PREFIX = "applyontest:";
	private static String fieldDelim = ",";
	private static String itemListDelim = "\\|";
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ARM Computation");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		ARMCompute armProc = new ARMCompute(args[0], args[1], args[2], args[3]);
		
		
		armProc.compute(sc);
	}

	public ARMCompute(String transFilePath, String outputDir,
			String flattenArg,
			String testDataArg) {
		this.transFilePath = transFilePath;
		this.outputDir = outputDir;
		String[] tokens = flattenArg.split(":");
		this.applyFlatten = Boolean.parseBoolean(tokens[1]);
		tokens = testDataArg.split(":");
		this.applyOnTestData = Boolean.parseBoolean(tokens[1]);
	}
	
	public void compute(JavaSparkContext sc) {
		System.out.println("ApplyOnTest:" + this.applyOnTestData);
		if(this.applyOnTestData)
			processTestData(sc);
		else
			processRetailTrans(sc);
	}
	
	public void printRetailTransSummary(JavaRDD<String> stringRdd) {
		
		System.out.println("Number of entries:" + stringRdd.count());
		// Header: Invoice	StockCode	Description	Quantity	InvoiceDate	Price	Customer ID	Country
		JavaRDD<Tuple8<String, String, String, String, String, String, String, String>> transDetailsRdd =
				stringRdd.map(x -> {
					String[] tokens = x.split(fieldDelim);
					return new Tuple8<>(tokens[0],tokens[1],tokens[2], tokens[3],
							tokens[4], tokens[5], tokens[6], tokens[7]);
				});
		long transCount = transDetailsRdd.map(x -> x._1()).distinct().count();
		long itemCodeCount = transDetailsRdd.map(x -> x._2()).distinct().count();
		long itemDescCount = transDetailsRdd.map(x -> x._2()).distinct().count();
		long customerCount = transDetailsRdd.map(x -> x._6()).distinct().count();
		List<String> countries = transDetailsRdd.map(x -> x._7()).distinct().collect();
		
		System.out.printf("Transaction count: %d, ItemCoude count: %d, ItemDesc count: %d, Customer count: %d\n",
				transCount, itemCodeCount, itemDescCount, customerCount );
		System.out.printf("Countries: \n %s \n", countries.toString());
	}
	
	public void processRetailTrans(JavaSparkContext sc) {

		// Header: Invoice	StockCode	Description	Quantity	InvoiceDate	Price	Customer ID	Country
		JavaRDD<String> stringRdd = sc.textFile(transFilePath).cache();
		
		printRetailTransSummary(stringRdd);

		// TransId, StockCode, Description
		JavaPairRDD<String, Tuple2<String, String>> transItemsRdd = 
				stringRdd.mapToPair(x -> {
					String[] tokens = x.split(fieldDelim);
					String transId = tokens[0];
					String itemCode = tokens[1];
					String itemDesc = tokens[2];
					
					/*System.out.printf("transId:%s, itemCode:%s, itemDesc:%s \n",
							transId, itemCode, itemDesc); */

					return new Tuple2<>(transId, new Tuple2<>(itemCode, itemDesc));
				});

		JavaPairRDD<String, String> itemsMapRdd = transItemsRdd.mapToPair(x -> x._2);
		JavaRDD<String> distinctItemsStr = 
				itemsMapRdd.map(x -> x._1 + "_" + x._2).distinct().cache();
		List<String> distinctItemsList = 
				distinctItemsStr.collect();
		
		System.out.println("Distinct items list..");
		distinctItemsList.forEach(System.out::println);
		
		JavaPairRDD<String, String> distinctItemsRdd = 
				distinctItemsStr.mapToPair(x -> {
					String[] tokens = x.split("_");
					String itemId = tokens[0];
					String itemDesc = (tokens.length>1)?tokens[1]:"NA";
					//System.out.println(".. tokens:" + tokens[0] +" .. " + tokens.length);
					return new Tuple2<>(itemId, itemDesc);
				});
		
		Map<String, String> itemsMap = distinctItemsRdd.collectAsMap();
		
		distinctItemsRdd.map(x -> x._1 + "," + x._2).saveAsTextFile(outputDir + "/ItemsMap");
		
		// <TransId, ItemId>
		JavaPairRDD<String, String> transFlatRdd =  
				transItemsRdd.mapToPair(x -> new Tuple2<>(x._1, x._2._1));
		long transCount = transFlatRdd.keys().distinct().count();
		
		JavaRDD<Rule> rulesRdd = 
				processFlatTransData(sc, transFlatRdd, transCount);
		
		JavaRDD<Rule> top100SupportRules = sc.parallelize(rulesRdd.top(100, new RulesSupportComparator()));
		JavaRDD<Rule> top100ConfidenceRules = sc.parallelize(rulesRdd.top(100, new RulesConfidenceComparator()));
		JavaRDD<Rule> top100LiftRules = sc.parallelize(rulesRdd.top(100, new RulesLiftComparator()));
		
		joinWithDescAndPrint(rulesRdd, itemsMap, "RulesWithItemDesc");
		
		joinWithDescAndPrint(top100SupportRules, itemsMap, "Top100SupportRules");
		joinWithDescAndPrint(top100ConfidenceRules, itemsMap, "Top100ConfidenceRules");
		joinWithDescAndPrint(top100LiftRules, itemsMap, "Top100LiftRules");
	}
	
	public void joinWithDescAndPrint(JavaRDD<Rule> rulesRdd,
			Map<String, String> itemsMap, 
			String outputType) {
		
		JavaRDD<String> toPrint = rulesRdd.map(x -> {
			String antItemDesc = itemsMap.get(x.getAntecedent());
			String consItemDesc = itemsMap.get(x.getConsequent());
			return x.toCsvString() + "," + antItemDesc + "," + consItemDesc;
		});
		
		toPrint.saveAsTextFile(outputDir + "/" + outputType);
	}
	
	public void processTestData(JavaSparkContext sc) {

		JavaRDD<String> stringRdd = sc.textFile(transFilePath).cache();

		JavaPairRDD<String, String> transFlat;
		long transCount;
		
		if(applyFlatten) {

			JavaPairRDD<String, Transaction> transRdd = 
					stringRdd.mapToPair(x -> {
						String[] tokens = x.split(fieldDelim);
						String transId = tokens[0];
						String[] itemIds = tokens[1].split(itemListDelim);
						List<String> itemIdList = new ArrayList<>();
						for(String itemId : itemIds)
							itemIdList.add(itemId);

						Transaction trans = new Transaction(transId, itemIdList);

						return new Tuple2<>(transId, trans); 
					});

			transFlat = transRdd.flatMapToPair(x -> {

				List<Tuple2<String, String>> tuples = new ArrayList<>();
				List<String> itemIds = x._2.getItemList();

				for(String itemId: itemIds)
					tuples.add(new Tuple2<>(x._1, itemId));

				return tuples.iterator();
			});
			
			transCount = transRdd.count();

		}else {
			transFlat = stringRdd.mapToPair(x -> {
				String[] tokens = x.split(fieldDelim);
				String transId = tokens[0];
				String itemId = tokens[1];
				
				return new Tuple2<>(transId, itemId);
			});
			
			transCount = transFlat.keys().distinct().count();
		}

		
		processFlatTransData(sc, transFlat, transCount);
	}
	
	public JavaRDD<Rule> processFlatTransData(JavaSparkContext sc,
			JavaPairRDD<String, String> transFlat,
			long transCount) {
		

		JavaPairRDD<String, Integer> itemFreqRdd = 
				transFlat.map(x -> x._2).mapToPair(x -> new Tuple2<>(x, 1)).reduceByKey((x,y) -> x+y);
		
		JavaPairRDD<String, Double> itemSupportRdd = 
				itemFreqRdd.mapToPair(x -> new Tuple2<>(x._1, x._2/(transCount*1.0)));
		itemSupportRdd.map(x -> x._1 + "," + x._2).saveAsTextFile(this.outputDir+"/PerItemSupport");

		Map<String, Double> itemSupportMap = itemSupportRdd.collectAsMap();

		JavaPairRDD<String, Tuple2<String, String>> joined = 
				transFlat.join(transFlat);
		// Removing entries joined with the same entries
		JavaPairRDD<String, Tuple2<String, String>> filtered = joined.filter(x -> 
		!(x._2._1.equals(x._2._2)));

		// Create directional rule <a,b> fields {<a,b>, <b,a>}
		JavaPairRDD<String, String> rulePairRdd = 
				filtered.mapToPair(x -> x._2);
		

		JavaPairRDD<Rule, Integer> ruleFreqRdd = 
				rulePairRdd.mapToPair(x -> new Tuple2<>(new Rule(x._1, x._2), 1) ).reduceByKey((x,y) -> x+y);
		
		JavaPairRDD<Rule, Double> ruleSupportRdd = 
				ruleFreqRdd.mapToPair(x ->{
					double support = x._2/(transCount*1.0);
					x._1.setSupport(support);
					return new Tuple2<>(x._1, support);
				}); 

		JavaRDD<Rule> rulesRdd = 
				ruleSupportRdd.map(x -> {
					Rule rule = x._1;
					rule.setAntSupport(itemSupportMap.get(rule.getAntecedent()));
					rule.setConsSupport(itemSupportMap.get(rule.getConsequent()));
					
					double confidence = x._2 / rule.getAntSupport();
					double lift = x._2 / ( rule.getAntSupport() * rule.getConsSupport());
					rule.setConfidence(confidence);
					rule.setLift(lift);

					return rule;
				});

		JavaRDD<String> toPrint =
				rulesRdd.map(x -> x.toCsvString());
		toPrint.saveAsTextFile(this.outputDir+"/RulesAndMetrics");
		
		printRulesStats(rulesRdd);
		
		return rulesRdd;
	}
	
	public void printRulesStats(JavaRDD<Rule> rulesRdd) {
		
		rulesRdd.map(x -> x.getAntecedent()).mapToPair(x -> new Tuple2<>(x, 1) ).reduceByKey((x,y) -> x+y)
			.map(x -> x._1 + "," + x._2).saveAsTextFile(this.outputDir + "/ItemRuleCount");
		long count = rulesRdd.count();
		double minSupport = rulesRdd.map(x -> x.getSupport()).min(new DoubleComparator());
		double maxSupport = rulesRdd.map(x -> x.getSupport()).max(new DoubleComparator());
		double sum = rulesRdd.map(x -> x.getSupport()).fold(0.0, (a,b) -> a+b);
		double meanSupport = sum / (count*1.0);
		
		double minConfidence = rulesRdd.map(x -> x.getConfidence()).min(new DoubleComparator());
		double maxConfidence = rulesRdd.map(x -> x.getConfidence()).max(new DoubleComparator());
		sum = rulesRdd.map(x -> x.getConfidence()).fold(0.0, (a,b) -> a+b);
		double meanConfidence = sum / (count*1.0);
		
		double minLift = rulesRdd.map(x -> x.getLift()).min(new DoubleComparator());
		double maxLift = rulesRdd.map(x -> x.getLift()).max(new DoubleComparator());
		sum = rulesRdd.map(x -> x.getLift()).fold(0.0, (a,b) -> a+b);
		double meanLift = sum / (count*1.0);
		
		System.out.println("Support stats");
		System.out.printf("\nMin:%f, Max:%f, Mean:%f\n", minSupport, maxSupport, meanSupport);
		
		System.out.println("Confidence stats");
		System.out.printf("\nMin:%f, Max:%f, Mean:%f\n", minConfidence, maxConfidence, meanConfidence);
		
		System.out.println("Lift stats");
		System.out.printf("\nMin:%f, Max:%f, Mean:%f\n", minLift, maxLift, meanLift);
	}
	
	public class DoubleComparator implements Serializable, Comparator<Double> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(Double o1, Double o2) {
			return o1.compareTo(o2);
		}
		
	}

	
	public class RulesSupportComparator implements Serializable, Comparator<Rule>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		

		@Override
		public int compare(Rule o1, Rule o2) {
			return Double.valueOf(o1.getSupport()).compareTo(Double.valueOf(o2.getSupport()));
		}
		
	}
	
	public class RulesConfidenceComparator implements Serializable, Comparator<Rule>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		

		@Override
		public int compare(Rule o1, Rule o2) {
			return Double.valueOf(o1.getConfidence()).compareTo(Double.valueOf(o2.getConfidence()));
		}
		
	}
	
	public class RulesLiftComparator implements Serializable, Comparator<Rule>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		

		@Override
		public int compare(Rule o1, Rule o2) {
			return Double.valueOf(o1.getLift()).compareTo(Double.valueOf(o2.getLift()));
		}
		
	}
	
}
