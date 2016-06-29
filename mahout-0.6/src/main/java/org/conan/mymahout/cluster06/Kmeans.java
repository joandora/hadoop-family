package org.conan.mymahout.cluster06;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansClusterer;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;

public class Kmeans {

    public static void main(String[] args) throws IOException {
        String file = Kmeans.class.getResource("/datafile/randomData.csv").getPath();
        List<Vector> sampleData = MathUtil.readFileToVector(file);

        int k = 3;
        double threshold = 0.01;

        List<Vector> randomPoints = MathUtil.chooseRandomPoints(sampleData, k);
        for (Vector vector : randomPoints) {
            System.out.println("Init Point center: " + vector);
        }

        List<Cluster> clusters = new ArrayList<Cluster>();
        for (int i = 0; i < k; i++) {
            clusters.add(new Cluster(randomPoints.get(i), i, new EuclideanDistanceMeasure()));
        }

        List<List<Cluster>> finalClusters = KMeansClusterer.clusterPoints(sampleData, clusters, new EuclideanDistanceMeasure(), k, threshold);
        for (Cluster cluster : finalClusters.get(finalClusters.size() - 1)) {
            System.out.println("Cluster id: " + cluster.getId() + " center: " + cluster.getCenter().asFormatString());
        }
    }
    /**
     * 结果
     * Init Point center: {0:-0.0267949528995186,1:2.18995716285231}
       Init Point center: {0:0.0533829908470105,1:2.32675793952058}
       Init Point center: {0:-0.079555689998197,1:1.66708798143465}
       Cluster id: 0 center: {0:-0.5698481426067924,1:1.9963771089863265}
       Cluster id: 1 center: {0:2.868927446073611,1:4.1303602126392835}
       Cluster id: 2 center: {0:1.2464149886773763,1:-0.6828685327739283}
     */

    /**
     * 解析
     * 1. Init Point center表示，kmeans算法初始时的设置的3个中心点
       2. Cluster center表示，聚类后找到3个中心点
     */

}
