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
import java.util.ArrayList;
import java.util.List;

import javax.naming.Context;


/*compile and run instrutions I used: 
javac -classpath $(hadoop classpath) taskE.java
jar cf taskE.jar taskE*.class
hdfs dfs -rm -r -f /user/ds503/project2/part2/output
hadoop jar taskE.jar taskE

View results:
look at shared folder on local computer
*/

public class taskE {
    
    public static class taskOutputCentroids extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] vals = line.split(",");

            //return each centroid and whether results converged 
            String centroidKey = vals[0] + "," + vals[1] + "," + vals[2] + "," + vals[3];

            context.write(new Text(centroidKey), new Text(context.getConfiguration().get("centroid.convergence")));
        }
    }

    public static class taskOutputAllPoints extends Mapper<Object, Text, Text, Text> {
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

    public static class taskEMapper extends Mapper<Object, Text, Text, Text>{
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

     public static class taskEReducer extends Reducer<Text,Text,Text,NullWritable> {

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

    //combined our inputs with someone elses function
    public static double euclideanDistance(double[] p1, double[] p2) {
        double sum = 0;
        for (int i = 0; i < 4; i++) {
            double diff = p1[i] - p2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
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

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        String centroidPath = "/user/ds503/centroids/centroids.txt";
        boolean result = true;
        int k = 3;
        int threshold = 2;

        List<double[]> prevCentroids = new ArrayList<>();
        List<double[]> currCentroids = new ArrayList<>();

        boolean convergence = false;
        boolean outputCentroids = false; //if set to false, output will be in the form of (centroid, point) for all data points
        
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
                
            job.setJarByClass(taskE.class);
    
            job.setMapperClass(taskEMapper.class);
            job.setReducerClass(taskEReducer.class);
    
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
    
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            
            FileInputFormat.setInputPaths(job, new Path("file:///home/ds503/data.txt"));
    
            String outputPath = "/user/ds503/project2/part2/output/output_iteration_" + i;
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
                convergence = true;
                break;
            }
        }

        //handle output with one final job
        Configuration conf = new Configuration();
        conf.set("centroid.convergence", convergence ? "Convergence reached" : "Convergence not reached");

        conf.set("centroid.path", centroidPath);
        Job job = Job.getInstance(conf);
            
        job.setJarByClass(taskE.class);

        if(outputCentroids) {
            job.setMapperClass(taskOutputCentroids.class);
            FileInputFormat.setInputPaths(job, new Path(centroidPath));
        }
        else {
            job.setMapperClass(taskOutputAllPoints.class);
            FileInputFormat.setInputPaths(job, new Path("file:///home/ds503/data.txt"));
        }

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);    

        String outputPath = "file:///home/ds503/shared_folder/project2/part2/partA/outputFinal";

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}