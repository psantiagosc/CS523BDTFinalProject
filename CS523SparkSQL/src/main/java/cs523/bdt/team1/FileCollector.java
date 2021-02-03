package cs523.bdt.team1;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.SparkSession;

public class FileCollector{
    
    static SparkConf conf;
    static JavaSparkContext ss;
    static Configuration hconf;
    static FileSystem fs;
    //Others
    final static String ABS_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera";
    static String initialPath;
    static String tablename;
    
    public static void merge() throws Exception{
        
       // if(args.length!=1) System.out.println("1 argument is necessary (time in milliseconds that this job will wait for running again)");
        //else{
        	conf = new SparkConf().setAppName("FileCollector").setMaster("local[2]");
//          SparkSession  ss = JavaSparkContext.builder().config(conf).enableHiveSupport().getOrCreate();
    		JavaSparkContext ss = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
    		System.out.println(ss==null);
            
            hconf = new Configuration();
            hconf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
            hconf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
            fs = FileSystem.get(hconf);
            initialPath = ABS_PATH+"/temp_info";
            tablename = "all_info";
            
            mergeFiles(ss);

            
//            while(true){
//                System.out.println("Running merge job again...");
//                mergeFiles(ss);
//                System.out.println("Merge job finished.");
//                Thread.sleep(Integer.valueOf("10000"));
//            }
        //}
    }
    
    public static void mergeFiles(JavaSparkContext ss) throws IOException{
        
        //WORK
        if(fs.exists(new Path(initialPath))){
            //get together all the files from .../initialPath/...
        	System.out.println(ss==null);
            JavaRDD<String> abc = ss.textFile(initialPath+"/*/*");
            //abc.collect().forEach(System.out::println);
            abc.coalesce(1).saveAsTextFile(ABS_PATH+"/hive_tables/temp_"+tablename);
            //store
            Path temp_path = new Path(ABS_PATH+"/hive_tables/temp_"+tablename);
            if(fs.exists(temp_path)){
                fs.rename(temp_path.suffix("/part-00000"), new Path(ABS_PATH+"/hive_tables/"+tablename+"/"+System.currentTimeMillis()));
                fs.delete(temp_path, true);
            }else System.out.println("There was a problem when sending the file to hive directory");
            //delete example content (i.e. all the files gotten from Spark Streaming)
            fs.delete(new Path(initialPath+"/*"), false);
        }
    }
}