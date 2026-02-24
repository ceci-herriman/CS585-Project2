import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.naming.Context;


//compile and run instrutions I used: 
// javac -classpath $(hadoop classpath) taskA.java
// jar cf taskA.jar taskA*.class
// rm -rf ~/shared_folder/project2/part2/partA/output
// hadoop jar taskA.jar taskA

public class taskA {
    
    //for optimal solution
    public static class taskAMapper extends Mapper<Object, Text, Text, Text>{
        List<double[]> seedsList = new ArrayList<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            // Load CircleNetPage.txt from Distributed Cache
           BufferedReader br = new BufferedReader(new FileReader("kseeds.txt"));
           //BufferedReader br = new BufferedReader(new Path("file:///home/ds503/shared_folder/project2/part2/partA/input/kseeds.txt"));

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

        private Double euclideanDistance(double[] point1, double[] point2) {
            double wDiff = Math.pow(point1[0] - point2[0], 2);
            double xDiff = Math.pow(point1[1] - point2[1], 2);
            double yDiff = Math.pow(point1[2] - point2[2], 2);
            double zDiff = Math.pow(point1[3] - point2[3], 2);

            double result = Math.sqrt(wDiff + xDiff + yDiff + zDiff);

            return result;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] vals = line.split(",");
            double[] pointDoubles = new double[4];

            for (int i = 0; i < 4; i++) {
                pointDoubles[i] = Double.parseDouble(vals[i]);
            }

            //for each seed in seedsList, calculate distance from point
            double minDistance = euclideanDistance(pointDoubles, seedsList.get(0));
            int minDistSeed = 0;
            for(int i = 1; i < seedsList.size(); i++) {
                double distance = euclideanDistance(pointDoubles, seedsList.get(i));
                if(distance < minDistance) {
                    minDistance = distance;
                    minDistSeed = i;
                }
            }

            //now we have the index of the seed which the point should go to
            //return <seed, line>
            String seedText = seedsList.get(minDistSeed).toString();
            context.write(new Text(seedText), new Text(line));
        }
    }


     public static class taskAReducer extends Reducer<Text,Text,Text,Iterable<Text>> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String centroid = key.toString(); 
            double totalW = 0;
            double totalX = 0;
            double totalY = 0;
            double totalZ = 0;
            int size = 0;

            for(Text val : values) {
                String[] sList = val.toString().split(","); //one point
                size++; 
                for(int i = 0; i < sList.length; i++) { //go through each coordinate in point
                    double coord = Double.parseDouble(sList[i]);
                    if(i == 0) {
                        totalW += coord;
                    }
                    else if(i == 1) {
                        totalX += coord;
                    }
                    else if(i == 2) {
                        totalY += coord;
                    }
                    else if(i == 3) {
                        totalZ += coord;
                    }
                }
            }

            double[] average = {(totalW / size), (totalX / size), (totalY / size), (totalZ / size)};
            context.write(new Text(average.toString()), values);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // job.setReduceSpeculativeExecution(false);

        job.setJarByClass(taskA.class);

        //simple version
        job.setMapperClass(taskAMapper.class);
        job.setReducerClass(taskAReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(job, new Path("file:///home/ds503/shared_folder/project2/part2/partA/input/data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("file:///home/ds503/shared_folder/project2/part2/partA/output"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
