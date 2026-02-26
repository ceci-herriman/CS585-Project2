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
javac -classpath $(hadoop classpath) taskA.java
jar cf taskA.jar taskA*.class
hdfs dfs -rm -r -f /user/ds503/project2/part2/output
hadoop jar taskA.jar taskA

View results:
hdfs dfs -cat /user/ds503/project2/part2/output/output_iteration_9/part-r-00000
*/

public class taskA {
    
    //for optimal solution
    public static class taskAMapper extends Mapper<Object, Text, Text, Text>{
        List<double[]> seedsList = new ArrayList<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            //BufferedReader br = new BufferedReader(new FileReader("kseeds.txt"));
            String centroidPath = context.getConfiguration().get("centroid.path");

            //             Configuration conf = new Configuration();

            // conf.set("fs.defaultFS", "hdfs://localhost:9000");
            Path pathObj = new Path(centroidPath);
            FileSystem fs = FileSystem.get(context.getConfiguration());

            // FSDataInputStream fsis = fs.open(path);
            // BufferedReader br = new BufferedReader(new InputStreamReader(fsis));
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

        //combined our inputs with someone elses function
        private double distanceSquared(double[] p1, double[] p2) {
            double sum = 0;
            for (int i = 0; i < 4; i++) {
                double diff = p1[i] - p2[i];
                sum += diff * diff;
            }
            return sum;
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
            double minDistance = distanceSquared(pointDoubles, seedsList.get(0)); //get distance from first seed to point
            for (int i = 1; i < seedsList.size(); i++) {
                double distance = distanceSquared(pointDoubles, seedsList.get(i));
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


     public static class taskAReducer extends Reducer<Text,Text,Text,NullWritable> {

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

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        String centroidPath = "/user/ds503/centroids/centroids.txt";
        boolean result = true;
        int k = 10;

        for(int i = 0; i < k; i++) {

            Configuration conf = new Configuration();
            conf.set("centroid.path", centroidPath);
            Job job = Job.getInstance(conf, "Iteration " + i);
            
            // job.setReduceSpeculativeExecution(false);
    
            job.setJarByClass(taskA.class);
    
            job.setMapperClass(taskAMapper.class);
            job.setReducerClass(taskAReducer.class);
    
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
    
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            
            FileInputFormat.setInputPaths(job, new Path("file:///home/ds503/data.txt"));
    
            String outputPath = "/user/ds503/project2/part2/output/output_iteration_" + i;
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
    
            result = job.waitForCompletion(true);
            
            centroidPath = outputPath + "/part-r-00000";
    
        }

        System.exit(result ? 0 : 1);
    }
}
