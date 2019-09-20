import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.vividsolutions.jts.geom.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;

import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;

public class main implements Serializable{
    /** The sc. */
    public static JavaSparkContext sc;

    /** The geometry factory. */
    static GeometryFactory gf = new GeometryFactory();

    /** The Point RDD input location. */
    static String PointRDDInputLocation;
    static String PointRDDInputLocation_2;
    /** The Point RDD offset. */
    static Integer PointRDDOffset;

    /** The Point RDD num partitions. */
    static Integer PointRDDNumPartitions;

    /** The Point RDD splitter. */
    static FileDataSplitter PointRDDSplitter;

    /** The Point RDD index type. */
    static IndexType PointRDDIndexType;

    /** The object RDD. */
    static PointRDD objectRDD;

    /** The Polygon RDD input location. */
    static String PolygonRDDInputLocation;

    /** The Polygon RDD start offset. */
    static Integer PolygonRDDStartOffset;

    /** The Polygon RDD end offset. */
    static Integer PolygonRDDEndOffset;

    /** The Polygon RDD num partitions. */
    static Integer PolygonRDDNumPartitions;

    /** The Polygon RDD splitter. */
    static FileDataSplitter PolygonRDDSplitter;

    /** The query window RDD. */
    static PolygonRDD queryWindowRDD;

    /** The join query partitioning type. */
    static GridType joinQueryPartitioningType;

    /** The each query loop times. */
    static int eachQueryLoopTimes = 10;

    /** The k NN query point. */
    static Point kNNQueryPoint;

    /** The range query window. */
    static Envelope rangeQueryWindow;

    static String ShapeFileInputLocation;

    static Random randomGenerator = new Random();

    static double min_x = -120.284557;
    static double max_x = -117.618988;
    static double min_y = 32.806193;
    static double max_y = 36.485821;

    static double range_x = min_x - max_x;
    static double range_y = min_y - max_y;

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        // Remote EMR master is .setMaster("yarn")
        SparkConf conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());

        long start = System.currentTimeMillis();


        String resourceFolder = "hdfs://34.222.72.136:54311/geospark/join/";
        PointRDDInputLocation = resourceFolder+"hdfs_1m_1.csv";
        PointRDDInputLocation_2 = resourceFolder+"hdfs_1m_2.csv";

        sc = new JavaSparkContext(conf);

        PointRDDSplitter = FileDataSplitter.CSV;
        PointRDDIndexType = IndexType.RTREE;
        PointRDDNumPartitions = 5;
        PointRDDOffset = 0;

        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());

        PointRDDNumPartitions = 5;
        PointRDDOffset = 0;
        long end = System.currentTimeMillis();
        System.out.println("Load Time:" + (end-start)/1000.0);

        joinQueryPartitioningType = GridType.QUADTREE;


        try {
            //testSpatialRangeQuery();
            //testSpatialRangeQueryUsingIndex();
            //testSpatialKnnQuery();
            //testSpatialKnnQueryUsingIndex();

            SpatialKnn_joinQuery();
            SpatialKnn_joinQueryUsingIndex();

        } catch (Exception e) {
            e.printStackTrace();
        }

        sc.stop();
    }

    public static void testSpatialRangeQuery() throws Exception {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());

        double total = 0;

        for(int i=0;i<eachQueryLoopTimes;i++)
        {

            double random_no_1 = randomGenerator.nextDouble() * range_x + max_x;
            double random_no_2 = randomGenerator.nextDouble() * range_x + max_x;
            double random_no_3 = randomGenerator.nextDouble() * range_y + max_y;
            double random_no_4 = randomGenerator.nextDouble() * range_y + max_y;

            double temp;

            if (random_no_1 > random_no_2){
                temp = random_no_2;
                random_no_2 = random_no_1;
                random_no_1 = temp;
            }

            if(random_no_3 > random_no_4){
                temp = random_no_4;
                random_no_4 = random_no_3;
                random_no_3 = temp;
            }

            rangeQueryWindow = new Envelope(random_no_1, random_no_2, random_no_3, random_no_4);

            long start = System.currentTimeMillis();

            long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,false).count();
            assert resultSize>-1;

            long end = System.currentTimeMillis();
            System.out.println(resultSize);
            double time_ = (end-start)/1000.0;

            total += time_;
            System.out.println("Range time : " + time_);
        }
        System.out.println("total : " + total);
    }

    public static void testSpatialRangeQueryUsingIndex() throws Exception {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        objectRDD.buildIndex(PointRDDIndexType,false);
        objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());

        double total = 0;

        for(int i=0;i<eachQueryLoopTimes;i++)
        {
            double random_no_1 = randomGenerator.nextDouble() * range_x + max_x;
            double random_no_2 = randomGenerator.nextDouble() * range_x + max_x;
            double random_no_3 = randomGenerator.nextDouble() * range_y + max_y;
            double random_no_4 = randomGenerator.nextDouble() * range_y + max_y;

            double temp;

            if (random_no_1 > random_no_2){
                temp = random_no_2;
                random_no_2 = random_no_1;
                random_no_1 = temp;
            }

            if(random_no_3 > random_no_4){
                temp = random_no_4;
                random_no_4 = random_no_3;
                random_no_3 = temp;
            }

            rangeQueryWindow = new Envelope(random_no_1, random_no_2, random_no_3, random_no_4);

            long start = System.currentTimeMillis();

            long resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,true).count();
            assert resultSize>-1;

            long end = System.currentTimeMillis();

            double time_ = (end-start)/1000.0;

            total += time_;
            System.out.println("Range index time : " + time_);
            System.out.println(resultSize);
        }
        System.out.println("total : " + total);
    }


    public static void testSpatialKnnQuery() throws Exception {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());

        double total = 0;

        for(int i=0;i<eachQueryLoopTimes;i++)
        {
            double random_no_1 = randomGenerator.nextDouble() * range_x + max_x;
            double random_no_3 = randomGenerator.nextDouble() * range_y + max_y;

            Coordinate coor = new Coordinate();
            coor.x = random_no_1;
            coor.y = random_no_3;

            kNNQueryPoint = gf.createPoint(coor);

            long start = System.currentTimeMillis();

            List<Point> result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000,false);
            assert result.size()>-1;

            long end = System.currentTimeMillis();

            double time_ = (end-start)/1000.0;
            total += time_;

            System.out.println("KNN Time:" + time_);

            System.out.println(result.size());
        }
        System.out.println("total : " + total);
    }


    public static void testSpatialKnnQueryUsingIndex() throws Exception {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        objectRDD.buildIndex(PointRDDIndexType,false);
        objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY());

        double total = 0;

        for(int i=0;i<eachQueryLoopTimes;i++)
        {
            double random_no_1 = randomGenerator.nextDouble() * range_x + max_x;
            double random_no_3 = randomGenerator.nextDouble() * range_y + max_y;

            Coordinate coor = new Coordinate();
            coor.x = random_no_1;
            coor.y = random_no_3;

            kNNQueryPoint = gf.createPoint(coor);

            long start = System.currentTimeMillis();

            List<Point> result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1000, true);
            assert result.size()>-1;

            long end = System.currentTimeMillis();

            double time_ = (end-start)/1000.0;
            total += time_;

            System.out.println("KNN index Time:" + time_);

            System.out.println(result.size());
        }
        System.out.println("total : " + total);
    }


    public static void SpatialKnn_joinQuery() throws Exception {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        //objectRDD = new PointRDD(rdd, StorageLevel.MEMORY_ONLY());
        objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());

        double total = 0;

        for(int i=0;i<11;i++) {
            System.out.println(i);
            double sub_total = 0;
            double random_no_1 = randomGenerator.nextDouble() * range_x + max_x;
            double random_no_3 = randomGenerator.nextDouble() * range_y + max_y;

            Coordinate coor = new Coordinate();
            coor.x = random_no_1;
            coor.y = random_no_3;

            kNNQueryPoint = gf.createPoint(coor);

            long start = System.currentTimeMillis();

            List<Point> result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 100,false);
            assert result.size()>-1;

            long end = System.currentTimeMillis();
            double time_ = (end-start)/1000.0;
            sub_total += time_;
            // ---------------------------------------------------------------------

            ArrayList<Coordinate> points = new ArrayList<Coordinate>();
            for(i=0; i<100; i++){
                points.add(new Coordinate(result.get(i).getX(), result.get(i).getY()));
            }

            points.add(new Coordinate(result.get(0).getX(), result.get(0).getY()));

            Polygon polygon_ = gf.createPolygon((Coordinate[]) points.toArray(new Coordinate[]{}));

            List<Polygon> aaaa= new ArrayList<>();
            aaaa.add(polygon_);
            JavaRDD<Polygon> jr = sc.parallelize(aaaa);

            double tot = testSpatialJoinQuery(jr, sub_total);


            total += tot;

        }
        System.out.println("total : " + total);

    }

    public static double testSpatialJoinQuery(JavaRDD<Polygon> jr, double sub_total) throws Exception {
        //queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true);

        queryWindowRDD = new PolygonRDD(jr, StorageLevel.MEMORY_ONLY());
        //objectRDD = new PointRDD(rdd2, StorageLevel.MEMORY_ONLY());
        objectRDD = new PointRDD(sc, PointRDDInputLocation_2, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());

        double return_time = 0;


        for (int i=0; i<11; i++) {
            double sub = sub_total;

            long start = System.currentTimeMillis();

            objectRDD.spatialPartitioning(joinQueryPartitioningType);
            queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());

            objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
            queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

            long resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, false, true).count();
            assert resultSize > 0;

            long end = System.currentTimeMillis();
            double time_ = (end - start) / 1000.0;
            sub += time_;
            return_time += sub;

            System.out.println("KNN-JOIN Time:" + sub);
        }
        return return_time;
    }


    public static void SpatialKnn_joinQueryUsingIndex() throws Exception {
        objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        //objectRDD = new PointRDD(rdd, StorageLevel.MEMORY_ONLY());
        objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());

        double total = 0;

        for(int i=0;i<1;i++)
        {
            System.out.println(i);
            double sub_total = 0;
            double random_no_1 = randomGenerator.nextDouble() * range_x + max_x;
            double random_no_3 = randomGenerator.nextDouble() * range_y + max_y;

            Coordinate coor = new Coordinate();
            coor.x = random_no_1;
            coor.y = random_no_3;

            kNNQueryPoint = gf.createPoint(coor);

            long start = System.currentTimeMillis();

            List<Point> result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 100,false);
            assert result.size()>-1;
            long end = System.currentTimeMillis();
            double time_ = (end-start)/1000.0;
            // ---------------------------------------------------------------------
            sub_total+= time_;

            ArrayList<Coordinate> points = new ArrayList<Coordinate>();
            for(i=0; i<100; i++){
                points.add(new Coordinate(result.get(i).getX(), result.get(i).getY()));
            }
            points.add(new Coordinate(result.get(0).getX(), result.get(0).getY()));

            Polygon polygon_ = gf.createPolygon((Coordinate[]) points.toArray(new Coordinate[]{}));

            List<Polygon> aaaa= new ArrayList<>();
            aaaa.add(polygon_);
            JavaRDD<Polygon> jr = sc.parallelize(aaaa);

            double tot = testSpatialJoinQueryUsingIndex(jr, sub_total);

            total += tot;
        }
        System.out.println("total : " + total);

    }
    /**
     * Test spatial join query using index.
     *
     * @throws Exception the exception
     */
    public static double testSpatialJoinQueryUsingIndex(JavaRDD<Polygon> jr, double sub_total) throws Exception {
        //queryWindowRDD = new PolygonRDD(sc, PolygonRDDInputLocation, PolygonRDDStartOffset, PolygonRDDEndOffset, PolygonRDDSplitter, true);

        queryWindowRDD = new PolygonRDD(jr, StorageLevel.MEMORY_ONLY());
        objectRDD = new PointRDD(sc, PointRDDInputLocation_2, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());

        double return_time = 0;

        for (int i=0; i<11; i++) {
            double sub = sub_total;

            long start = System.currentTimeMillis();
            objectRDD.spatialPartitioning(joinQueryPartitioningType);
            queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());

            objectRDD.buildIndex(PointRDDIndexType,true);

            objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
            queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

            long resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, true, false).count();
            assert resultSize > 0;
            long end = System.currentTimeMillis();
            double time_ = (end - start) / 1000.0;
            sub += time_;
            return_time += sub;

            System.out.println("KNN-JOIN using index time :" + sub);
        }
        return return_time;

    }
}
