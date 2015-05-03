package task1; /**
 * Created by guichi on 9/04/2015.
 */


import task1.job1.*;
import task1.job2.CountMapper;
import task1.job2.CountReducer;
import task1.job3.MySortGroupComparator;
import task1.job3.SortMapper;
import task1.job3.SortPartitioner;
import task1.job4.MaxMapper;
import task1.job4.MaxPartitioner;
import task1.job6.TagSortMapper;
import task1.job6.TagSortPartitioner;
import task1.job7.TagMaxMapper;
import task1.job8.FinalMapper;
import task1.job8.FinalReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import task1.job5.*;
import task1.job1.PhotoFilterMapper;

public class MRDriverTask1 {

    public static void main(String[] args) throws Exception {
         Boolean local=true;

        Configuration conf = new Configuration();
        if (args.length != 1) {
            System.err.println("Usage: MRDriverTask1 <stage>");
            System.exit(2);
        }

        int stage = Integer.parseInt(args[0]);

        System.out.println(stage);
        if (stage <= 1) {
            Job job = new Job(conf, "join place and 1 GB photo with  8 reducer  ");
            //job.setNumReduceTasks(10);
            job.setJarByClass(MRDriverTask1.class);

            Path photoTable;
            Path placeTable;

            if (local)
            {
             photoTable = new Path("photo");
                placeTable = new Path("place");
                MultipleInputs.addInputPath(job, photoTable,
                        TextInputFormat.class, PhotoFilterMapper.class);
            }
            else
            {
                placeTable=new Path("/share/place.txt");
                Path[] photoTables={
                        new Path("/share/large/n01.txt"),new Path("/share/large/n04.txt")};
                for (Path path:photoTables) {
                    MultipleInputs.addInputPath(job, path,
                            TextInputFormat.class, PhotoFilterMapper.class);
                }

            }



            Path outputTable = new Path("t1joinOutput");
//            MultipleInputs.addInputPath(job, photoTable,
//                    TextInputFormat.class, PhotoFilterMapper.class);
            MultipleInputs.addInputPath(job, placeTable, TextInputFormat.class, PlaceFilterMapper.class);
            FileOutputFormat.setOutputPath(job, outputTable);
            job.setMapOutputKeyClass(PlaceOrderPair.class);
            job.setNumReduceTasks(8);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setGroupingComparatorClass(JoinGroupComparator.class);
            job.setReducerClass(JoinReducer.class);
            job.setPartitionerClass(JoinPartitioner.class);


            job.waitForCompletion(true);
        }


        if (stage <= 2) {
            Job countJob = Job.getInstance();
            countJob.setJarByClass(MRDriverTask1.class);

            Path inputPath = new Path("t1joinOutput");
            Path outputPath = new Path("countOutput");

            FileInputFormat.setInputPaths(countJob, inputPath);
            FileOutputFormat.setOutputPath(countJob, outputPath);

            countJob.setMapperClass(CountMapper.class);
            countJob.setNumReduceTasks(1);
            countJob.setReducerClass(CountReducer.class);

            countJob.setOutputKeyClass(Text.class);
            countJob.setOutputValueClass(Text.class);


            countJob.waitForCompletion(true);

        }


        if (stage <= 3) {

            Job sortJob = Job.getInstance();
            sortJob.setJarByClass(MRDriverTask1.class);

            Path sortInput = new Path("countOutput");
            Path sortOutput = new Path("sortOutput");
            FileInputFormat.setInputPaths(sortJob, sortInput);
            FileOutputFormat.setOutputPath(sortJob, sortOutput);

            sortJob.setMapperClass(SortMapper.class);
            sortJob.setNumReduceTasks(5);

            sortJob.setPartitionerClass(SortPartitioner.class);
            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);

            sortJob.setSortComparatorClass(MySortGroupComparator.class);

            sortJob.waitForCompletion(true);
        }

        if (stage <= 4) {
            Job maxJob = Job.getInstance();
            maxJob.setJarByClass(MRDriverTask1.class);

            Path maxInput = new Path("sortOutput/part-r-00000");
            Path maxOutput = new Path("maxOutput");
            FileInputFormat.setInputPaths(maxJob, maxInput);
            FileOutputFormat.setOutputPath(maxJob, maxOutput);

            maxJob.setMapperClass(MaxMapper.class);
            maxJob.setPartitionerClass(MaxPartitioner.class);
            maxJob.setNumReduceTasks(51);

            maxJob.setOutputKeyClass(IntWritable.class);
            maxJob.setOutputValueClass(Text.class);

            maxJob.waitForCompletion(true);
        }

        if (stage <= 5) {
            Job tagJob = Job.getInstance();
            tagJob.setJarByClass(MRDriverTask1.class);

            Path tagInput = new Path("maxOutput");
            Path tagOutput = new Path("tagOutput");
            FileInputFormat.setInputPaths(tagJob, tagInput);
            FileOutputFormat.setOutputPath(tagJob, tagOutput);

            tagJob.setMapOutputKeyClass(Text.class);
            tagJob.setMapOutputValueClass(IntWritable.class);

            tagJob.setMapperClass(TagMapper.class);
            tagJob.setPartitionerClass(TagPartitioner.class);
            tagJob.setCombinerClass(TagReducer.class);
            //tagJob.setReducerClass(TagReducer.class);
            tagJob.setNumReduceTasks(51);


            tagJob.setOutputKeyClass(Text.class);
            tagJob.setOutputValueClass(IntWritable.class);


            tagJob.waitForCompletion(true);


        }
        if (stage <= 6) {

            Job tagSortJob = Job.getInstance();
            tagSortJob.setJarByClass(MRDriverTask1.class);

            Path tagSortInput = new Path("tagOutput");
            Path tagSortOutput = new Path("tagSortOutput");
            FileInputFormat.setInputPaths(tagSortJob, tagSortInput);
            FileOutputFormat.setOutputPath(tagSortJob, tagSortOutput);

            tagSortJob.setMapOutputKeyClass(IntWritable.class);
            tagSortJob.setMapOutputValueClass(Text.class);

            tagSortJob.setMapperClass(TagSortMapper.class);
            tagSortJob.setPartitionerClass(TagSortPartitioner.class);
            tagSortJob.setSortComparatorClass(MySortGroupComparator.class);
            //tagJob.setCombinerClass(TagReducer.class);
            //tagJob.setReducerClass(TagReducer.class);
            tagSortJob.setNumReduceTasks(51);


            tagSortJob.setOutputKeyClass(Text.class);
            tagSortJob.setOutputValueClass(Text.class);

            tagSortJob.waitForCompletion(true);

        }

        if (stage <= 7) {
            Job tagMaxJob = Job.getInstance();
            tagMaxJob.setJarByClass(MRDriverTask1.class);

            Path tagMaxInput = new Path("tagSortOutput");
            Path tagMaxOutput = new Path("tagMaxOutput");
            FileInputFormat.setInputPaths(tagMaxJob, tagMaxInput);
            FileOutputFormat.setOutputPath(tagMaxJob, tagMaxOutput);

            tagMaxJob.setMapOutputKeyClass(IntWritable.class);
            tagMaxJob.setMapOutputValueClass(Text.class);

            tagMaxJob.setMapperClass(TagMaxMapper.class);
            //tagMaxJob.setPartitionerClass(TagSortPartitioner.class);
            //tagMaxJob.setSortComparatorClass(MySortGroupComparator.class);
            //tagJob.setCombinerClass(TagReducer.class);
            //tagJob.setReducerClass(TagReducer.class);
            tagMaxJob.setNumReduceTasks(1);


            //tagMaxJob.setOutputKeyClass(Text.class);
            //tagMaxJob.setOutputValueClass(Text.class);
            tagMaxJob.waitForCompletion(true);


        }

        if (stage<=8)
        {
            Job finalJob = Job.getInstance();
            finalJob.setJarByClass(MRDriverTask1.class);

            Path finalInput = new Path("tagMaxOutput");
            Path finalOutput = new Path("t1finalOutput");
            FileInputFormat.setInputPaths(finalJob, finalInput);
            FileOutputFormat.setOutputPath(finalJob, finalOutput);

            finalJob.setMapOutputKeyClass(IntWritable.class);
            finalJob.setMapOutputValueClass(Text.class);

            finalJob.setMapperClass(FinalMapper.class);

            finalJob.setReducerClass(FinalReducer.class);
            finalJob.setNumReduceTasks(1);

            System.exit(finalJob.waitForCompletion(true) ? 0 : 1);

        }


    }
}



