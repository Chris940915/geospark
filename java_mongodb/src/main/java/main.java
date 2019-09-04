import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import static org.apache.spark.sql.functions.callUDF;

import org.apache.spark.api.java.function.Function;
import scala.collection.JavaConversions;
import com.mongodb.spark.config.ReadConfig;
import com.vividsolutions.jts.geom.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.codehaus.janino.Java;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geosparksql.utils.Adapter;
import org.bson.Document;
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import scala.collection.mutable.WrappedArray;
import java.util.ArrayList;
import java.util.Random;

public class main implements Serializable{

    public static JavaSparkContext sc;

    /** The geometry factory. */
    static GeometryFactory geometryFactory;

    /** The Point RDD input location. */
    static String PointRDDInputLocation;

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
    static int eachQueryLoopTimes = 5;

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

    static GeometryFactory gf = new GeometryFactory();


    public static void main(String[] args) throws Exception {

//        SparkSession spark = SparkSession.builder()
//                .master("local")
//                .appName("MongoSparkConnectorIntro")
//                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/spark.test2")
//                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/spark.test2")
//                .getOrCreate();

        SparkSession spark = SparkSession.builder()
                .master("spark://ec2-52-12-47-15.us-west-2.compute.amazonaws.com")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/spark.test2")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/spark.test2")
                .getOrCreate();

        sc = new JavaSparkContext(spark.sparkContext());

        JavaMongoRDD<Document> rdd = MongoSpark.load(sc);

//        Coordinate coor = new Coordinate();
//        coor.x = 151.1;
//        coor.y = 149.2;
//        Point p = gf.createPoint(coor);
//
//        p.getCoordinateSequence();


        Dataset<Row> implicitDS = MongoSpark.load(sc).toDF();

        //JavaRDD<Point> javardd = Adapter.toJavaRdd(implicitDS);

//        SpatialRDD spatialRDD = Adapter.toSpatialRdd(implicitDS);
//        JavaRDD<com.mongodb.client.model.geojson.Point> pointrdd = spatialRDD.getRawSpatialRDD();
//        testSpatialRangeQuery(pointrdd);


        implicitDS.createOrReplaceTempView("test1");
        Dataset<Row> implicit_centenar = spark.sql("SELECT loc FROM test1");

        long start = System.currentTimeMillis();
        //explicit Dataset
        Dataset<Character> explicitDS = MongoSpark.load(sc).toDS(Character.class);
        explicitDS.createOrReplaceTempView("test2");
        Dataset<Row> explicit_centenar = spark.sql("SELECT x, y FROM test2");

        List<Point> asdf = explicit_centenar.toJavaRDD().map(new Function<Row, Point>() {
            public Point call(Row row){
                Coordinate coor = new Coordinate();
                coor.x = row.getDouble(0);
                coor.y = row.getDouble(1);
                Point return_point = gf.createPoint(coor);

                return return_point;}
        }).collect();
        JavaRDD<Point> point_Rdd = sc.parallelize(asdf);

        objectRDD = new PointRDD(point_Rdd, StorageLevel.MEMORY_ONLY());
        objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());

        long end = System.currentTimeMillis();

        testSpatialRangeQuery(point_Rdd);
        System.out.println("Load Time:" + (end-start)/1000.0);

        //testSpatialRangeQueryUsingIndex(point_Rdd);
        //testSpatialKnnQuery(point_Rdd);
        //testSpatialKnnQueryUsingIndex(point_Rdd);

        sc.close();

    }

    public static void testSpatialRangeQuery(JavaRDD<Point> rdd) throws Exception {
        //objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());

        objectRDD = new PointRDD(rdd, StorageLevel.MEMORY_ONLY());
        objectRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());

        double total = 0;

        for(int i=0;i<eachQueryLoopTimes;i++){

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

            double time_ = (end-start)/1000.0;

            total += time_;
            System.out.println("time : " + time_);

            //System.out.println(resultSize);
        }

        System.out.println("total : " + total);
    }

    public static void testSpatialRangeQueryUsingIndex(JavaRDD<Point> rdd) throws Exception {
        //objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        objectRDD = new PointRDD(rdd, StorageLevel.MEMORY_ONLY());
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
            System.out.println("time : " + time_);
            System.out.println(resultSize);
        }
        System.out.println("total : " + total);
    }

    public static void testSpatialKnnQuery(JavaRDD<Point> rdd) throws Exception {
        //objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        objectRDD = new PointRDD(rdd, StorageLevel.MEMORY_ONLY());
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

            List<Point> result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 100,false);
            assert result.size()>-1;

            long end = System.currentTimeMillis();

            double time_ = (end-start)/1000.0;
            total += time_;

            System.out.println("KNN Time:" + time_);

            System.out.println(result.size());
        }
        System.out.println("total : " + total);

    }


    public static void testSpatialKnnQueryUsingIndex(JavaRDD<Point> rdd) throws Exception {
        //objectRDD = new PointRDD(sc, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, true, StorageLevel.MEMORY_ONLY());
        objectRDD = new PointRDD(rdd, StorageLevel.MEMORY_ONLY());
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

            List<Point> result = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 100, true);
            assert result.size()>-1;

            long end = System.currentTimeMillis();

            double time_ = (end-start)/1000.0;
            total += time_;

            System.out.println("KNN index Time:" + time_);

            System.out.println(result.size());
        }
        System.out.println("total : " + total);

    }
}

final class Character implements Serializable {
    private double x;
    private double y;
    private double[] loc;

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

}
