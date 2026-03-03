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
javac -classpath $(hadoop classpath) taskD.java
jar cf taskD.jar taskD*.class
hdfs dfs -rm -r -f /user/ds503/project2/part2/partD/output
hdfs dfs -rm -r -f /user/ds503/project2/part2/partD/silhouetteOutput
hadoop jar taskD.jar taskD

View results:
hdfs dfs -cat /user/ds503/project2/part2/partD/output/output_iteration_1/part-r-00000
hdfs dfs -cat /user/ds503/project2/part2/partD/silhouetteOutput/part-r-00000
*/

public class taskD {
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
    
    // MAPPER -optimized with hashmap and partial sums
    public static class taskDMapper extends Mapper<Object, Text, Text, Text>{
        List<double[]> seedsList = new ArrayList<>();
        HashMap<String, double[]> partialSums = new HashMap<>();

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

            // put partial sums in hashmap
            double[] accumulate = partialSums.get(centroidKey);
            if (accumulate == null) {
                accumulate = new double[5]; // wxyz, count]
                partialSums.put(centroidKey, accumulate);
            }
            accumulate[0] += pointDoubles[0];
            accumulate[1] += pointDoubles[1];
            accumulate[2] += pointDoubles[2];
            accumulate[3] += pointDoubles[3];
            accumulate[4] += 1;
        }
            protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, double[]> entry : partialSums.entrySet()) {
                double[] accumulate = entry.getValue();
                String partialVal = accumulate[0] + "," + accumulate[1] + "," + accumulate[2] + "," + accumulate[3] + "," + (double) accumulate[4];
                context.write(new Text(entry.getKey()), new Text(partialVal));
            }
        }
    }

    // COMBINER
    public static class taskDCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalW = 0;
            double totalX = 0;
            double totalY = 0;
            double totalZ = 0;
            long count = 0;

            for (Text val : values) {
                String[] sList = val.toString().split(",");
                totalW += Double.parseDouble(sList[0]);
                totalX += Double.parseDouble(sList[1]);
                totalY += Double.parseDouble(sList[2]);
                totalZ += Double.parseDouble(sList[3]);
                count += Long.parseLong(sList[4]);
            }

            String combinedVal = totalW + "," + totalX + "," + totalY + "," + totalZ + "," + count;
            context.write(key, new Text(combinedVal));
        }
    }



    // REDUCER
     public static class taskDReducer extends Reducer<Text,Text,Text,NullWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String centroid = key.toString(); 
            double totalW = 0;
            double totalX = 0;
            double totalY = 0;
            double totalZ = 0;
            long totalCount = 0;            

            for (Text val : values) {
                String[] sList = val.toString().split(",");
                totalW += Double.parseDouble(sList[0]);
                totalX += Double.parseDouble(sList[1]);
                totalY += Double.parseDouble(sList[2]);
                totalZ += Double.parseDouble(sList[3]);
                totalCount += Long.parseLong(sList[4]);            
            }


            String newCentroid =
                (totalW / totalCount) + "," +
                (totalX / totalCount) + "," +
                (totalY / totalCount) + "," +
                (totalZ / totalCount);
                
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

    // SILHOUETTE MAPPER - optimized to 1 Map only job to remove shuffling
    public static class SilhouetteMapper extends Mapper<Object, Text, Text, NullWritable> {
        List<double[]> allCentroids = new ArrayList<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            // load job1 centroid output
            URI[] cacheFiles = context.getCacheFiles();
            String centroidFileName = new Path(cacheFiles[0].toString()).getName();

            // modified from taskDMapper
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

            // for each seed in seedsList, calculate distance from point (a)
            int closest = 0; // track closest seed
            double a = euclideanDistance(pointDoubles, allCentroids.get(0)); // get distance from first seed to point
            for (int i = 1; i < allCentroids.size(); i++) {
                double distance = euclideanDistance(pointDoubles, allCentroids.get(i));
                if (distance < a) {
                    a = distance;
                    closest = i;
                }
            }

            // get distance to nearett other centroid (b)
            double b = Double.MAX_VALUE;
            for (int i = 0; i < allCentroids.size(); i++) {
                if (i != closest) {
                    double dist = euclideanDistance(pointDoubles, allCentroids.get(i));
                    if (dist < b)
                        b = dist;
                }
            }
            if (b == Double.MAX_VALUE)
                b = 0;

            double silhouetteSum = 0.0;
            // formula is (b - a) / max(a, b)
            double s;
            if (a == 0 && b == 0) {
                s = 0;
            } else {
                s = (b - a) / Math.max(a, b);
            }
            silhouetteSum += s;

            context.write(new Text(String.valueOf(silhouetteSum)), NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, java.net.URISyntaxException {

        String centroidPath = "/user/ds503/centroids/kseeds2.txt";
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
    
            job.setJarByClass(taskD.class);
    
            job.setMapperClass(taskDMapper.class);
            job.setCombinerClass(taskDCombiner.class);
            job.setReducerClass(taskDReducer.class);
    
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
    
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            
            FileInputFormat.setInputPaths(job, new Path("file:///home/ds503/data.txt"));
    
            String outputPath = "/user/ds503/project2/part2/partD/output/output_iteration_" + i;
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

        /*if (!result) {
            System.exit(1);
        }*/
        System.exit(result ? 0 : 1);

        // SILOHUETTE JOB
        /*
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);
                        
        long startTime2 = System.nanoTime();

        job2.setJarByClass(taskD.class);

        // simple version
        job2.setMapperClass(SilhouetteMapper.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(NullWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job2, new Path("file:///home/ds503/data.txt"));
        job2.addCacheFile(new URI(centroidPath)); // add final interation output of job 1 to cache
        FileOutputFormat.setOutputPath(job2, new Path("/user/ds503/project2/part2/partD/silhouetteOutput"));

        boolean result2 = job2.waitForCompletion(true);
                
        long endTime2 = System.nanoTime();
        durationMilli = (double) (endTime2 - startTime2) / 1000000.0;
        System.out.println("Time to complete in milliseconds: " + durationMilli);

        System.exit(result2 ? 0 : 1);*/
    }
}