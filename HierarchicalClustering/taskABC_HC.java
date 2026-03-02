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

/*compile and run instrutions I used:
javac -classpath $(hadoop classpath) taskABC_HC.java
jar cf taskABC_HC.jar taskABC_HC*.class
rm -rf ~/shared_folder/project2/part2/partA/output
hdfs dfs -rm -r -f /user/ds503/project2/part2/partA/output
hadoop jar taskABC_HC.jar taskABC_HC

retrieve ouputs:
cat ~/shared_folder/project2/part2/partA/output/part-r-00000
cat ~/shared_folder/project2/part2/partA/silhouetteOutput/part-r-00000
*/

public class taskABC_HC {

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
    public static void main(String[] args)
        throws IOException, InterruptedException, ClassNotFoundException {
        long startTime = System.nanoTime();

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf); // HDFS
        FileSystem localFs = FileSystem.getLocal(conf); // Local

        // in HC, each point starts as its own cluster
        List<double[]> centroids = new ArrayList<>();
        BufferedReader dataReader = new BufferedReader(new InputStreamReader(localFs.open(new Path("file:///home/ds503/dataHC.txt"))));
        String dataLine;
        while ((dataLine = dataReader.readLine()) != null) {
            dataLine = dataLine.trim();
            if (!dataLine.isEmpty())
                centroids.add(stringToCentroid(dataLine));
        }
        dataReader.close();

        // overwrite kseeds to keep track of centroids
        Path centroidHDFS = new Path("/user/ds503/centroids/kseeds.txt");
        if (fs.exists(centroidHDFS))
            fs.delete(centroidHDFS, false);
        BufferedWriter initWriter =
            new BufferedWriter(new OutputStreamWriter(fs.create(centroidHDFS)));
        for (double[] c : centroids) {
            initWriter.write(centroidToString(c));
            initWriter.newLine();
        }
        initWriter.close();

        // Merge until 1 cluster remains
        int round = 0;
        while (centroids.size() > 1) {
            String outputPath = "/user/ds503/project2/part2/partA/output/output_round_" + round;
            Path outPath = new Path(outputPath);
            if (fs.exists(outPath))
                fs.delete(outPath, true);

            Configuration roundConf = new Configuration();
            Job job = Job.getInstance(roundConf, "HC Round " + round);

            job.setJarByClass(taskABC_HC.class);

            job.setMapperClass(HACMapper.class);
            job.setReducerClass(HACReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job,
                new Path("file:///home/ds503/dataHC.txt")); // I only used 200 points because there will always be n-1 iterations, and 6000 is too long for me
            FileOutputFormat.setOutputPath(job, outPath);

            boolean result = job.waitForCompletion(true);
            if (!result)
                System.exit(1);

            // Read new centroids from last round
            centroids.clear();
            BufferedReader roundReader =
                new BufferedReader(new InputStreamReader(
                    fs.open(new Path(outputPath + "/part-r-00000"))));
            String roundLine;
            while ((roundLine = roundReader.readLine()) != null) {
                roundLine = roundLine.trim();
                if (!roundLine.isEmpty())
                    centroids.add(stringToCentroid(roundLine));
            }
            roundReader.close();

            // find closest cluster and merge
            int mergeA = -1, mergeB = -1;
            double minDistance = Double.MAX_VALUE;
            for (int i = 0; i < centroids.size(); i++) {
                for (int j = i + 1; j < centroids.size(); j++) {
                    double distance = euclideanDistance(centroids.get(i), centroids.get(j));
                    if (distance < minDistance) {
                        minDistance = distance;
                        mergeA = i;
                        mergeB = j;
                    }
                }
            }
            if (mergeA < 0)
                break;

            // only keep 1 centroid from the merged group
            double[] cA = centroids.get(mergeA);
            double[] cB = centroids.get(mergeB);
            double[] merged = new double[4];
            for (int i = 0; i < 4; i++) merged[i] = (cA[i] + cB[i]) / 2.0;

            centroids.remove(Math.max(mergeA, mergeB));
            centroids.remove(Math.min(mergeA, mergeB));
            centroids.add(merged);

            // Overwrite kseeds with updated centroids for next round
            if (fs.exists(centroidHDFS))
                fs.delete(centroidHDFS, false);
            BufferedWriter roundWriter = new BufferedWriter(new OutputStreamWriter(fs.create(centroidHDFS)));
            for (double[] centroid : centroids) {
                roundWriter.write(centroidToString(centroid));
                roundWriter.newLine();
            }
            roundWriter.close();

            round++;
        }

        // Final pass
        String finalOutputPath = "/user/ds503/project2/part2/partA/output/HC_final";
        Path finalOutPath = new Path(finalOutputPath);
        if (fs.exists(finalOutPath))
            fs.delete(finalOutPath, true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "HC Final");

        job2.setJarByClass(taskABC_HC.class);

        job2.setMapperClass(HACMapper.class);
        job2.setReducerClass(HACReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job2, new Path("file:///home/ds503/dataHC.txt"));
        FileOutputFormat.setOutputPath(job2, finalOutPath);

        boolean result = job2.waitForCompletion(true);

        long endTime = System.nanoTime();
        double durationMilli = (double) (endTime - startTime) / 1000000.0;

        // taskABC, taskB, taskC -
        // number of iterations is always the same (keep merging until 1 cluster exists is what HC is)
        System.out.println("Number of rounds to complete: " + round);
        System.out.println("Time to complete in milliseconds: " + durationMilli);

        System.exit(result ? 0 : 1);
    }
}