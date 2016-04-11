package com.mfniu.example.mahout.cf;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/*
 * 
 * 使用Mahout实现协同过滤:http://www.tuicool.com/articles/FzmQziz
 * 
 * Mahout推荐算法API详解:http://blog.csdn.net/zhoubl668/article/details/13297663/
 * 
 */
public class UserCFApp {

	public static void main(String[] args) throws TasteException, IOException {

		File modelFile = new File("intro.csv");

		DataModel model = new FileDataModel(modelFile);

		// 用户相似度，使用基于皮尔逊相关系数计算相似度
		UserSimilarity similarity = new PearsonCorrelationSimilarity(model);

		// 选择邻居用户，使用NearestNUserNeighborhood实现UserNeighborhood接口，选择邻近的4个用户
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(4, similarity, model);

		Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);

		// 给用户1推荐4个物品
		List<RecommendedItem> recommendations = recommender.recommend(1, 4);

		// 打印出推荐信息
		for (RecommendedItem recommendation : recommendations) {
			System.out.println(recommendation);
		}

		// 获取推荐模型的评分
		// 使用平均绝对差值获得评分
		RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		// 用RecommenderBuilder构建推荐引擎
		RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
			public Recommender buildRecommender(DataModel model) throws TasteException {
				UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
				UserNeighborhood neighborhood = new NearestNUserNeighborhood(4, similarity, model);
				return new GenericUserBasedRecommender(model, neighborhood, similarity);
			}
		};
		// Use 70% of the data to train; test using the other 30%.
		double score = evaluator.evaluate(recommenderBuilder, null, model, 0.7, 1.0);
		System.out.println(score);

		// 获取推荐结果的查准率和召回率
		RecommenderIRStatsEvaluator statsEvaluator = new GenericRecommenderIRStatsEvaluator();
		// Build the same recommender for testing that we did last time:
		RecommenderBuilder recommenderBuilder1 = new RecommenderBuilder() {
			public Recommender buildRecommender(DataModel model) throws TasteException {
				UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
				UserNeighborhood neighborhood = new NearestNUserNeighborhood(4, similarity, model);
				return new GenericUserBasedRecommender(model, neighborhood, similarity);
			}
		};
		// 计算推荐4个结果时的查准率和召回率
		IRStatistics stats = statsEvaluator.evaluate(recommenderBuilder1, null, model, null, 4,
				GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
		System.out.println(stats.getPrecision());
		System.out.println(stats.getRecall());

	}

}
