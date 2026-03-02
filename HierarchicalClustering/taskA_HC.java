import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.util.*;

public class taskA_HC {

    // SHARED MATH
    private static double euclideanDistance(double[] p1, double[] p2) {
        double sum = 0;
        for (int i = 0; i < 4; i++) {
            double diff = p1[i] - p2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    // turned repeated functions into helper funtions
    private static String centroidToString(double[] centroidDoubles) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            if (i > 0) sb.append(",");
            sb.append(centroidDoubles[i]);
        }
        return sb.toString();
    }

    private static double[] stringToCentroid(String s) {
        String[] parts = s.split(",");
        double[] centroidDoubles = new double[4];
        for (int i = 0; i < 4; i++) centroidDoubles[i] = Double.parseDouble(parts[i]);
        return centroidDoubles;
    }

    // MAPPER
    public static class HACMapper extends Mapper<Object, Text, Text, Text> {
        List<double[]> seedsList = new ArrayList<>();

        protected void setup(Context context) throws IOException {
            Path pathObj = new Path("/user/ds503/centroids/kseeds.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pathObj)));
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) seedsList.add(stringToCentroid(line));
            }
            br.close();
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            double[] pointDoubles = stringToCentroid(line);

            int closest = 0;
            double minDistance = euclideanDistance(pointDoubles, seedsList.get(0));
            for (int i = 1; i < seedsList.size(); i++) {
                double distance = euclideanDistance(pointDoubles, seedsList.get(i));
                if (distance < minDistance) { minDistance = distance; closest = i; }
            }

            context.write(new Text(centroidToString(seedsList.get(closest))), value);
        }
    }

    // REDUCER
    public static class HACReducer extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double[] sum = new double[4];
            int count = 0;

            for (Text val : values) {
                double[] pointDoubles = stringToCentroid(val.toString().trim());
                for (int i = 0; i < 4; i++) sum[i] += pointDoubles[i];
                count++;
            }

            double[] newCentroid = new double[4];
            for (int i = 0; i < 4; i++) newCentroid[i] = sum[i] / count;

            context.write(new Text(centroidToString(newCentroid)), NullWritable.get());
        }
    }

    // DRIVER
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        long startTime = System.nanoTime();

        FileSystem fs = FileSystem.get(new Configuration());

        // Each point starts as its own cluster
        List<double[]> centroids = new ArrayList<>();
        BufferedReader dataReader = new BufferedReader(
                new InputStreamReader(fs.open(new Path("file:///home/ds503/data.txt"))));
        String dataLine;
        while ((dataLine = dataReader.readLine()) != null) {
            dataLine = dataLine.trim();
            if (!dataLine.isEmpty()) centroids.add(stringToCentroid(dataLine));
        }
        dataReader.close();

        // overwriting kseeds to keep track of all centroids
        Path centroidHDFS = new Path("/user/ds503/centroids/kseeds.txt");
        if (fs.exists(centroidHDFS)) fs.delete(centroidHDFS, false);
        BufferedWriter initWriter = new BufferedWriter(new OutputStreamWriter(fs.create(centroidHDFS)));
        for (double[] centroidDoubles : centroids) { initWriter.write(centroidToString(centroidDoubles)); initWriter.newLine(); }
        initWriter.close();

        // merge to closest other cluster until 1 cluster remains
        int round = 0;
        while (centroids.size() > 1) {
            Path outPath = new Path("file:///home/ds503/shared_folder/project2/part2/partA/output/round_" + round);
            if (fs.exists(outPath)) fs.delete(outPath, true);

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);            
            
            job.setJarByClass(taskA_HC.class);

            job.setMapperClass(HACMapper.class);
            job.setReducerClass(HACReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job, new Path("file:///home/ds503/data.txt"));
            FileOutputFormat.setOutputPath(job, new Path("file:///home/ds503/shared_folder/project2/part2/partA/output/round_" + round));

            boolean result = job.waitForCompletion(true);

            if (!result) {
                System.exit(1);
            }

            // Read from reducer output
            centroids.clear();
            BufferedReader roundReader = new BufferedReader(
                    new InputStreamReader(fs.open(new Path("file:///home/ds503/shared_folder/project2/part2/partA/output/round_" + round + "/part-r-00000"))));
            String roundLine;
            while ((roundLine = roundReader.readLine()) != null) {
                roundLine = roundLine.trim();
                if (!roundLine.isEmpty()) centroids.add(stringToCentroid(roundLine));
            }
            roundReader.close();

            // distance matrix: calc all possible distances
            // find the shortest and merge those two clusters
            int mergeA = -1, mergeB = -1;
            double minDistance = Double.MAX_VALUE;
            for (int i = 0; i < centroids.size(); i++) {
                for (int j = i + 1; j < centroids.size(); j++) {
                    double distance = euclideanDistance(centroids.get(i), centroids.get(j));
                    if (distance < minDistance) { minDistance = distance; mergeA = i; mergeB = j; }
                }
            }
            if (mergeA < 0) break;

            double[] cA = centroids.get(mergeA);
            double[] cB = centroids.get(mergeB);
            double[] merged = new double[4];
            for (int i = 0; i < 4; i++) merged[i] = (cA[i] + cB[i]) / 2.0;

            //single link merge: only keep the closest point to the other clusters
            centroids.remove(Math.max(mergeA, mergeB));
            centroids.remove(Math.min(mergeA, mergeB));
            centroids.add(merged);

            // overwrite kseeds with new centroids for next round
            if (fs.exists(centroidHDFS)) fs.delete(centroidHDFS, false);
            BufferedWriter roundWriter = new BufferedWriter(new OutputStreamWriter(fs.create(centroidHDFS)));
            for (double[] centroidDoubles : centroids) { roundWriter.write(centroidToString(centroidDoubles)); roundWriter.newLine(); }
            roundWriter.close();

            round++;
        }

        // JOB 2: Final pass
        Path finalOutPath = new Path("file:///home/ds503/shared_folder/project2/part2/partA/output/HC_final");
        if (fs.exists(finalOutPath)) fs.delete(finalOutPath, true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);        
        
        job2.setJarByClass(taskA_HC.class);

        job2.setMapperClass(HACMapper.class);
        job2.setReducerClass(HACReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job2, new Path("file:///home/ds503/data.txt"));
        FileOutputFormat.setOutputPath(job2, new Path("file:///home/ds503/shared_folder/project2/part2/partA/output/HC_final"));

        boolean result = job2.waitForCompletion(true);

        long endTime = System.nanoTime();
        double durationMilli = (double)(endTime - startTime) / 1000000.0;
        System.out.println("Time to complete in milliseconds: " + durationMilli);
        
        System.exit(result ? 0 : 1);
    }
}