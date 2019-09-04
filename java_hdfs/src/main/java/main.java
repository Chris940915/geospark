import java.io.Serializable;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;

public class main implements Serializable{
    /** The sc. */
    public static JavaSparkContext sc;

    /** The geometry factory. */
    static GeometryFactory gf = new GeometryFactory();

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

        String resourceFolder = "hdfs:///geospark/resources/";

        PointRDDInputLocation = resourceFolder+"streets.csv";

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
            testSpatialRangeQuery();
            testSpatialRangeQueryUsingIndex();
            testSpatialKnnQuery();
            testSpatialKnnQueryUsingIndex();
        } catch (Exception e) {
            e.printStackTrace();
        }
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
}
