package org.conan.mymahout.recommendation.book;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.conan.mymahout.hdfs.HdfsDAO;
/**
 * 与BookResult类似，不过增加了过滤器，只保留男性用户的图书列表<br>
 * 由于只保留男性的评分记录，数据量就变得比较少了，基于用户的协同过滤算法，已经没有输出的结果了。基于物品的协同过滤算法，结果集也有所变化<br>
 * itemEuclideanNoPref给用户65最先推荐887的图书<br>
 * 再查看rating.csv发现，有4个用户都给887打过分，单只有两名男性：184和186<br>
 * 通过分析发现：用户65与用户186都给图书65和图书375打过分、用户186，还给图书887打过分，所以对于给65用户推荐图书887，是合理的<br>
 */
public class BookFilterGenderResult {

    final static int NEIGHBORHOOD_NUM = 2;
    final static int RECOMMENDER_NUM = 3;

    public static void main(String[] args) throws TasteException, IOException {
        String localFile = BookFilterGenderResult.class.getResource("/datafile/book/rating.csv").getPath();
        DataModel dataModel = RecommendFactory.buildDataModel(localFile);
        RecommenderBuilder rb1 = BookEvaluator.userEuclidean(dataModel);
        RecommenderBuilder rb2 = BookEvaluator.itemEuclidean(dataModel);
        RecommenderBuilder rb3 = BookEvaluator.userEuclideanNoPref(dataModel);
        RecommenderBuilder rb4 = BookEvaluator.itemEuclideanNoPref(dataModel);
        
        long uid = 65;
        System.out.print("userEuclidean       =>");
        filterGender(uid, rb1, dataModel);
        System.out.print("itemEuclidean       =>");
        filterGender(uid, rb2, dataModel);
        System.out.print("userEuclideanNoPref =>");
        filterGender(uid, rb3, dataModel);
        System.out.print("itemEuclideanNoPref =>");
        filterGender(uid, rb4, dataModel);
    }

    /**
     * 对用户性别进行过滤
     */
    public static void filterGender(long uid, RecommenderBuilder recommenderBuilder, DataModel dataModel) throws TasteException, IOException {
        String localFile = BookFilterGenderResult.class.getResource("/datafile/book/user.csv").getPath();
        Set<Long> userids = getMale(localFile);

        //计算男性用户打分过的图书
        Set<Long> bookids = new HashSet<Long>();
        for (long uids : userids) {
            LongPrimitiveIterator iter = dataModel.getItemIDsFromUser(uids).iterator();
            while (iter.hasNext()) {
                long bookid = iter.next();
                bookids.add(bookid);
            }
        }

        IDRescorer rescorer = new FilterRescorer(bookids);
        List<RecommendedItem> list = recommenderBuilder.buildRecommender(dataModel).recommend(uid, RECOMMENDER_NUM, rescorer);
        RecommendFactory.showItems(uid, list, false);
    }

    /**
     * 获得男性用户ID
     */
    public static Set<Long> getMale(String file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(file)));
        Set<Long> userids = new HashSet<Long>();
        String s = null;
        while ((s = br.readLine()) != null) {
            String[] cols = s.split(",");
            if (cols[1].equals("M")) {// 判断男性用户
                userids.add(Long.parseLong(cols[0]));
            }
        }
        br.close();
        return userids;
    }

}

/**
 * 对结果重计算
 */
class FilterRescorer implements IDRescorer {
    final private Set<Long> userids;

    public FilterRescorer(Set<Long> userids) {
        this.userids = userids;
    }

    @Override
    public double rescore(long id, double originalScore) {
        return isFiltered(id) ? Double.NaN : originalScore;
    }

    @Override
    public boolean isFiltered(long id) {
        return userids.contains(id);
    }

    /**
     *
     userEuclidean
     AVERAGE_ABSOLUTE_DIFFERENCE Evaluater Score:0.0
     Recommender IR Evaluator: [Precision:0.3010752688172043,Recall:0.08542713567839195]
     itemEuclidean
     AVERAGE_ABSOLUTE_DIFFERENCE Evaluater Score:1.374159288367415
     Recommender IR Evaluator: [Precision:0.0,Recall:0.0]
     userEuclideanNoPref
     AVERAGE_ABSOLUTE_DIFFERENCE Evaluater Score:5.0110146223780605
     Recommender IR Evaluator: [Precision:0.09045226130653267,Recall:0.09296482412060306]
     itemEuclideanNoPref
     AVERAGE_ABSOLUTE_DIFFERENCE Evaluater Score:2.543966384663835
     Recommender IR Evaluator: [Precision:0.6005025125628134,Recall:0.6055276381909548]
     userEuclidean       =>uid:65,
     itemEuclidean       =>uid:65,(784,8.090909)(276,8.000000)(476,7.666667)
     userEuclideanNoPref =>uid:65,
     itemEuclideanNoPref =>uid:65,(887,2.250000)(356,2.166667)(430,1.866667)**/
}
