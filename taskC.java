import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.net.URI;

/*compile and run instrutions I used: 
javac -classpath $(hadoop classpath) taskC.java
jar cf taskC.jar taskC*.class
hdfs dfs -rm -r -f /user/ds503/project2/part2/partC/output
hdfs dfs -rm -r -f /user/ds503/project2/part2/partC/silhouetteOutput
hadoop jar taskC.jar taskC

View results:
hdfs dfs -cat /user/ds503/project2/part2/partC/output/output_iteration_1/part-r-00000
hdfs dfs -cat /user/ds503/project2/part2/partC/silhouetteOutput/part-r-00000
*/

public class taskC {
    // SHARED MATH - EUCLIDEAN DISTANCE
    // combined our inputs with someone elses function
    private static double euclideanDistance(double[] p1, double[] p2) {
        double sum = 0;
        for (int i = 0; i < 4; i++) {
            double diff = p1[i] - p2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    // claude wrote this function
    private static double averageDistance(double[] point, List<double[]> otherPoints, boolean excludeSelf) {
        double sum = 0;
        int count = 0;
        for (double[] other : otherPoints) {
            if (excludeSelf && Arrays.equals(point, other))
                continue;
            sum += euclideanDistance(point, other);
            count++;
        }
        return count == 0 ? 0 : sum / count;
    }
    
    // MAPPER
    public static class taskCMapper extends Mapper<Object, Text, Text, Text>{
        List<double[]> seedsList = new ArrayList<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            //BufferedReader br = new BufferedReader(new FileReader("kseeds.txt"));
            String centroidPath = context.getConfiguration().get("centroid.path");

            Path pathObj = new Path(centroidPath);
            FileSystem fs = FileSystem.get(context.getConfiguration());

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pathObj)));

            String line;
            while ((line = br.readLine()) != null) {
                String[] vals = line.split(",");
                double[] pointDoubles = new double[4];

                for (int i = 0; i < 4; i++) {
                    pointDoubles[i] = Double.parseDouble(vals[i]);
                }
                seedsList.add(pointDoubles);
            }
            br.close();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] vals = line.split(",");
            double[] pointDoubles = new double[4];

            for (int i = 0; i < 4; i++) {
                pointDoubles[i] = Double.parseDouble(vals[i]);
            }

            //for each seed in seedsList, calculate distance from point

            int closest = 0; //track closest seed
            double minDistance = euclideanDistance(pointDoubles, seedsList.get(0)); //get distance from first seed to point
            for (int i = 1; i < seedsList.size(); i++) {
                double distance = euclideanDistance(pointDoubles, seedsList.get(i));
                if (distance < minDistance) {
                    minDistance = distance;
                    closest = i;
                }
            }

            //now we have the index of the seed which the point should go to
            //return <seed, line>
            double[] seed = seedsList.get(closest);
            String centroidKey = seed[0] + "," + seed[1] + "," + seed[2] + "," + seed[3];

            context.write(new Text(centroidKey), value);
        }
    }

    // REDUCER
     public static class taskCReducer extends Reducer<Text,Text,Text,NullWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String centroid = key.toString(); 
            double totalW = 0;
            double totalX = 0;
            double totalY = 0;
            double totalZ = 0;
            int size = 0;

            for (Text val : values) {
                String[] sList = val.toString().split(",");
                totalW += Double.parseDouble(sList[0]);
                totalX += Double.parseDouble(sList[1]);
                totalY += Double.parseDouble(sList[2]);
                totalZ += Double.parseDouble(sList[3]);
                size++;
            }

            String newCentroid =
                (totalW / size) + "," +
                (totalX / size) + "," +
                (totalY / size) + "," +
                (totalZ / size);
            context.write(new Text(newCentroid), NullWritable.get()); //text is centroid "id" and nullwritable contains the wxyz coords
        }
    }
    
    public static List<double[]> loadCentroids(String path, Configuration conf) throws IOException, InterruptedException {
        List<double[]> centroidsList = new ArrayList<>();

        Path pathObj = new Path(path);
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pathObj)));

        String line;
        while ((line = br.readLine()) != null) {
            String[] vals = line.split(",");
            double[] pointDoubles = new double[4];

            for (int i = 0; i < 4; i++) {
                pointDoubles[i] = Double.parseDouble(vals[i]);
            }
            centroidsList.add(pointDoubles);
        }
        br.close();

        return centroidsList;
    }


    /*
        SILHOUETTE ANALYSIS
    */

    // SILHOUETTE MAPPER - similar to taskCMapper but the input file is the result of the knn job
    public static class SilhouetteMapper extends Mapper<Object, Text, Text, Text> {
        List<double[]> allCentroids = new ArrayList<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            // load job1 centroid output
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("No centroid file found in distributed cache.");
            }
            String centroidFileName = new Path(cacheFiles[0].toString()).getName();

            // modified from taskCMapper
            BufferedReader br = new BufferedReader(new FileReader(centroidFileName));
            String line;
            while ((line = br.readLine()) != null) {
                String[] vals = line.split(",");
                double[] centroidDoubles = new double[4];
                for (int i = 0; i < 4; i++) {
                    centroidDoubles[i] = Double.parseDouble(vals[i]);
                }
                allCentroids.add(centroidDoubles);
            }
            br.close();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] vals = line.split(",");
            double[] pointDoubles = new double[4];

            for (int i = 0; i < 4; i++) {
                pointDoubles[i] = Double.parseDouble(vals[i]);
            }

            // for each seed in seedsList, calculate distance from point
            int closest = 0; // track closest seed
            double minDistance = euclideanDistance(pointDoubles, allCentroids.get(0)); // get distance from first seed to point
            for (int i = 1; i < allCentroids.size(); i++) {
                double distance = euclideanDistance(pointDoubles, allCentroids.get(i));
                if (distance < minDistance) {
                    minDistance = distance;
                    closest = i;
                }
            }

            // now we have the index of which cluster the point belongs to
            // return <seed, line>
            double[] seed = allCentroids.get(closest);
            String clusterKey = seed[0] + "," + seed[1] + "," + seed[2] + "," + seed[3];

            context.write(new Text(clusterKey), value);
        }
    }

    // SILHOUETTE REDUCER
    public static class SilhouetteReducer extends Reducer<Text, Text, Text, NullWritable> {
        List<double[]> allCentroids = new ArrayList<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            // load centroids output from job 1 - written with claude
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("No centroid file found in distributed cache.");
            }
            String centroidFileName = new Path(cacheFiles[0].toString()).getName();

            // similar to taskCMapper setup
            BufferedReader br = new BufferedReader(new FileReader(centroidFileName));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty())
                    continue;
                String[] vals = line.split(",");
                double[] centroidDouble = new double[4];
                for (int i = 0; i < 4; i++) {
                    centroidDouble[i] = Double.parseDouble(vals[i]);
                }
                allCentroids.add(centroidDouble);
            }
            br.close();
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // get cluster's centroid from key
            String[] keyVals = key.toString().split(",");
            double[] currentCentroid = new double[4];
            for (int i = 0; i < 4; i++) {
                currentCentroid[i] = Double.parseDouble(keyVals[i]);
            }

            List<double[]> clusterPoints = new ArrayList<>();
            for (Text val : values) {
                String[] valueStrings = val.toString().trim().split(",");
                double[] pointDoubles = new double[4];
                for (int i = 0; i < 4; i++) {
                    pointDoubles[i] = Double.parseDouble(valueStrings[i]);
                }
                clusterPoints.add(pointDoubles);
            }

            //list of other centroids (except this cluster's centroid)
            List<double[]> otherCentroids = new ArrayList<>();
            for (double[] centroidDoubles : allCentroids) {
                if (!Arrays.equals(centroidDoubles, currentCentroid)) {
                    otherCentroids.add(centroidDoubles);
                }
            }

            double silhouetteSum = 0.0;

            for (double[] point : clusterPoints) {
                double a = averageDistance(point, clusterPoints, true);
                double b = Double.MAX_VALUE;
                for (double[] otherCentroid : otherCentroids) {
                    double dist = euclideanDistance(point, otherCentroid);
                    b = Math.min(b, dist);
                }
                if (b == Double.MAX_VALUE)
                    b = 0;
                
                // formula is (b - a) / max(a, b)
                double s; 
                if (a == 0 && b == 0) {
                    s = 0;
                } else {
                    s = (b - a) / Math.max(a, b);
                }
                silhouetteSum += s;
            }

            double avgSilhouette = silhouetteSum / clusterPoints.size();
            context.write(new Text(String.valueOf(avgSilhouette)), NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, java.net.URISyntaxException {

        String centroidPath = "/user/ds503/centroids/centroids.txt";
        boolean result = true;
        int k = 5;
        int threshold = 2000;
        long startTime = System.nanoTime();


        List<double[]> prevCentroids = new ArrayList<>();
        List<double[]> currCentroids = new ArrayList<>();
        
        for(int i = 0; i < k; i++) {

            Configuration conf = new Configuration();

            //if this is the first loop, we should load centroids from the path, if not, we can copy from list of prev iteration
            if(currCentroids.size() == 0) {
                prevCentroids = loadCentroids(centroidPath, conf);
            }
            else {
                prevCentroids = currCentroids;
            }

            conf.set("centroid.path", centroidPath);
            Job job = Job.getInstance(conf, "Iteration " + i);
            
            // job.setReduceSpeculativeExecution(false);
    
            job.setJarByClass(taskC.class);
    
            job.setMapperClass(taskCMapper.class);
            job.setReducerClass(taskCReducer.class);
    
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
    
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            
            FileInputFormat.setInputPaths(job, new Path("file:///home/ds503/data.txt"));
    
            String outputPath = "/user/ds503/project2/part2/partC/output/output_iteration_" + i;
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
    
            result = job.waitForCompletion(true);
            
            centroidPath = outputPath + "/part-r-00000";

            currCentroids = loadCentroids(centroidPath, conf);

            //compare each center in each centroid list --> get distance between each pair
            boolean shouldTerminate = true; 

            for(int j = 0; j < currCentroids.size(); j++) {
                double minDist = Double.MAX_VALUE;

                for(int l = 0; l < prevCentroids.size(); l++) {
                    double distance = euclideanDistance(currCentroids.get(j), prevCentroids.get(l));
                    if(distance < minDist) {
                        minDist = distance;
                    }
                }
                
                if(minDist > threshold) {
                    //if the distance is more than the threshold, then we need to iterate again
                    shouldTerminate = false; 
                }

                System.out.println("Iteration " + i + " and centroids " + j + "     have distance " + minDist);
                String centroidKey = currCentroids.get(j)[0] + "," + currCentroids.get(j)[1] + "," + currCentroids.get(j)[2] + "," + currCentroids.get(j)[3];
                String prevCentroid = prevCentroids.get(j)[0] + "," + prevCentroids.get(j)[1] + "," + prevCentroids.get(j)[2] + "," + prevCentroids.get(j)[3];
                System.out.println("curr centroid: " + centroidKey);
                System.out.println("prev centroid: " + prevCentroid);

            }

            if(shouldTerminate) {
                break;
            }
    
        }

        long endTime = System.nanoTime();
        double durationMilli = (double) (endTime - startTime) / 1000000.0;
        System.out.println("Time to complete in milliseconds: " + durationMilli);

        if (!result) {
            System.exit(1);
        }

        // SILOHUETTE JOB
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);
                        
        long startTime2 = System.nanoTime();;

        job2.setJarByClass(taskC.class);

        // simple version
        job2.setMapperClass(SilhouetteMapper.class);
        job2.setReducerClass(SilhouetteReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job2, new Path("file:///home/ds503/data.txt"));
        job2.addCacheFile(new URI(centroidPath)); // add final interation output of job 1 to cache
        FileOutputFormat.setOutputPath(job2, new Path("/user/ds503/project2/part2/partC/silhouetteOutput"));

        boolean result2 = job2.waitForCompletion(true);
                
        long endTime2 = System.nanoTime();
        durationMilli = (double) (endTime2 - startTime2) / 1000000.0;
        System.out.println("Time to complete in milliseconds: " + durationMilli);

        System.exit(result2 ? 0 : 1);
    }
}