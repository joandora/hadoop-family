package org.conan.mymahout.recommendation.book;

import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
/**选出“评估推荐器”验证得分较高的算法<br>
 * itemEuclideanNoPref评估的结果最好<br>
 **/
public class BookEvaluator {

    final static int NEIGHBORHOOD_NUM = 2;
    final static int RECOMMENDER_NUM = 3;

    public static void main(String[] args) throws TasteException, IOException {
        String localFile = BookFilterGenderResult.class.getResource("/datafile/book/rating.csv").getPath();
        DataModel dataModel = RecommendFactory.buildDataModel(localFile);
        userEuclidean(dataModel);
        userLoglikelihood(dataModel);
        userEuclideanNoPref(dataModel);
        itemEuclidean(dataModel);
        itemLoglikelihood(dataModel);
        itemEuclideanNoPref(dataModel); // 根据结果 评估的结果最好
        slopeOne(dataModel);
    }

    public static RecommenderBuilder userEuclidean(DataModel dataModel) throws TasteException, IOException {
        System.out.println("userEuclidean");
        UserSimilarity userSimilarity = RecommendFactory.userSimilarity(RecommendFactory.SIMILARITY.EUCLIDEAN, dataModel);
        UserNeighborhood userNeighborhood = RecommendFactory.userNeighborhood(RecommendFactory.NEIGHBORHOOD.NEAREST, userSimilarity, dataModel, NEIGHBORHOOD_NUM);
        RecommenderBuilder recommenderBuilder = RecommendFactory.userRecommender(userSimilarity, userNeighborhood, true);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        return recommenderBuilder;
    }
    
    public static RecommenderBuilder userLoglikelihood(DataModel dataModel) throws TasteException, IOException {
        System.out.println("userLoglikelihood");
        UserSimilarity userSimilarity = RecommendFactory.userSimilarity(RecommendFactory.SIMILARITY.LOGLIKELIHOOD, dataModel);
        UserNeighborhood userNeighborhood = RecommendFactory.userNeighborhood(RecommendFactory.NEIGHBORHOOD.NEAREST, userSimilarity, dataModel, NEIGHBORHOOD_NUM);
        RecommenderBuilder recommenderBuilder = RecommendFactory.userRecommender(userSimilarity, userNeighborhood, true);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        return recommenderBuilder;
    }
    
    public static RecommenderBuilder userEuclideanNoPref(DataModel dataModel) throws TasteException, IOException {
        System.out.println("userEuclideanNoPref");
        UserSimilarity userSimilarity = RecommendFactory.userSimilarity(RecommendFactory.SIMILARITY.EUCLIDEAN, dataModel);
        UserNeighborhood userNeighborhood = RecommendFactory.userNeighborhood(RecommendFactory.NEIGHBORHOOD.NEAREST, userSimilarity, dataModel, NEIGHBORHOOD_NUM);
        RecommenderBuilder recommenderBuilder = RecommendFactory.userRecommender(userSimilarity, userNeighborhood, false);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        return recommenderBuilder;
    }

    public static RecommenderBuilder itemEuclidean(DataModel dataModel) throws TasteException, IOException {
        System.out.println("itemEuclidean");
        ItemSimilarity itemSimilarity = RecommendFactory.itemSimilarity(RecommendFactory.SIMILARITY.EUCLIDEAN, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.itemRecommender(itemSimilarity, true);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        return recommenderBuilder;
    }

    public static RecommenderBuilder itemLoglikelihood(DataModel dataModel) throws TasteException, IOException {
        System.out.println("itemLoglikelihood");
        ItemSimilarity itemSimilarity = RecommendFactory.itemSimilarity(RecommendFactory.SIMILARITY.LOGLIKELIHOOD, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.itemRecommender(itemSimilarity, true);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        return recommenderBuilder;
    }
    
    public static RecommenderBuilder itemEuclideanNoPref(DataModel dataModel) throws TasteException, IOException {
        System.out.println("itemEuclideanNoPref");
        ItemSimilarity itemSimilarity = RecommendFactory.itemSimilarity(RecommendFactory.SIMILARITY.EUCLIDEAN, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.itemRecommender(itemSimilarity, false);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        return recommenderBuilder;
    }

    public static RecommenderBuilder slopeOne(DataModel dataModel) throws TasteException, IOException {
        System.out.println("slopeOne");
        RecommenderBuilder recommenderBuilder = RecommendFactory.slopeOneRecommender();

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        return recommenderBuilder;
    }
    /*
    *
userEuclidean
AVERAGE_ABSOLUTE_DIFFERENCE Evaluater Score:0.08333331346511841
Recommender IR Evaluator: [Precision:0.3010752688172043,Recall:0.08542713567839195]
userLoglikelihood
AVERAGE_ABSOLUTE_DIFFERENCE Evaluater Score:1.8290335337320962
Recommender IR Evaluator: [Precision:0.11764705882352945,Recall:0.017587939698492466]
userEuclideanNoPref
AVERAGE_ABSOLUTE_DIFFERENCE Evaluater Score:4.438433652832394
Recommender IR Evaluator: [Precision:0.09045226130653267,Recall:0.09296482412060306]
itemEuclidean
AVERAGE_ABSOLUTE_DIFFERENCE Evaluater Score:1.4149588317756185
Recommender IR Evaluator: [Precision:0.0,Recall:0.0]
itemLoglikelihood
AVERAGE_ABSOLUTE_DIFFERENCE Evaluater Score:2.402807165823069
Recommender IR Evaluator: [Precision:0.0,Recall:0.0]
itemEuclideanNoPref
AVERAGE_ABSOLUTE_DIFFERENCE Evaluater Score:2.5113286750771153
Recommender IR Evaluator: [Precision:0.6005025125628134,Recall:0.6055276381909548]
slopeOne
AVERAGE_ABSOLUTE_DIFFERENCE Evaluater Score:2.7985169042221254
Recommender IR Evaluator: [Precision:0.0,Recall:0.0]
*/

}
