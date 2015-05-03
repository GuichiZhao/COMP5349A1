package task2;

import task2.job1.PhotoFilterMapper;
import task2.job1.PhotoRemoveDuplicateReducer;
import task2.job2.PlaceFilterMapper;
import task2.job3.*;
import task2.job4.GroupNeighbourMapper;
import task2.job4.NeighbourReducer;
import task2.job5.CountAndMaxReducer;
import task2.job5.GroupLocalityMapper;
import task2.job6.CountryGroupComparator;
import task2.job6.CountryLocalityCountPair;
import task2.job6.Top10CountryReducer;
import task2.job6.Top10LocalityMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by guichi on 19/04/2015.
 */
public class MRDriverTask2 {

    public static void main(String[] args) throws Exception{

        int stage=Integer.parseInt(args[0]);

        boolean loacal=true;


        if (stage<=1)
        {

            Job photoFilterJob=new Job();
            photoFilterJob.setJarByClass(MRDriverTask2.class);
            photoFilterJob.setJar("/usr/local/Cellar/hadoop/a1/t2/bin/t2.jar");
            photoFilterJob.setJobName("Filter Photo 10.24 GB with 80 reducers");



            if (loacal) {
                Path photoInputPath = new Path("photo");
                FileInputFormat.setInputPaths(photoFilterJob,photoInputPath);

            }else
            {

                Path[] photoInputPaths={
                        new Path("/share/large/n01.txt"),

                        };

                FileInputFormat.setInputPaths(photoFilterJob,photoInputPaths);
            }





            Path photoOutputPath=new Path("photoOutput");
            FileOutputFormat.setOutputPath(photoFilterJob, photoOutputPath);


            photoFilterJob.setMapperClass(PhotoFilterMapper.class);
            photoFilterJob.setReducerClass(PhotoRemoveDuplicateReducer.class);
            photoFilterJob.setNumReduceTasks(80);
            photoFilterJob.setOutputKeyClass(Text.class);
            photoFilterJob.setOutputValueClass(Text.class);


            photoFilterJob.waitForCompletion(true);


        }

        if (stage<=2)
        {
            Job placeFilterJob=new Job();
            placeFilterJob.setJobName("Filter place");
            placeFilterJob.setJarByClass(MRDriverTask2.class);

            Path placeInputPath;
            if (loacal) {
                placeInputPath = new Path("place");
            }else
            {
                placeInputPath=new Path("/share/place.txt");

            }
                Path placeOutputPath=new Path("placeOutput");

            FileInputFormat.setInputPaths(placeFilterJob, placeInputPath);
            FileOutputFormat.setOutputPath(placeFilterJob, placeOutputPath);

            placeFilterJob.setMapperClass(PlaceFilterMapper.class);
            placeFilterJob.setNumReduceTasks(1);
            placeFilterJob.setOutputKeyClass(Text.class);
            placeFilterJob.setOutputValueClass(Text.class);

            placeFilterJob.waitForCompletion(true);

        }

        if (stage<=3)
        {
            Job joinJob=new Job();
            joinJob.setJobName("Join");
            joinJob.setJarByClass(MRDriverTask2.class);
            Path photoJoinInput=new Path("photoOutput");
            Path placeJoinInput=new Path("placeOutput");

            Path joinOutput=new Path("joinOutput");

            MultipleInputs.addInputPath(joinJob, photoJoinInput,
                    TextInputFormat.class, PhotoJoinMapper.class);
            MultipleInputs.addInputPath(joinJob, placeJoinInput,
                    TextInputFormat.class, PlaceJoinMapper.class);
            FileOutputFormat.setOutputPath(joinJob, joinOutput);
            joinJob.setMapOutputKeyClass(PlaceOrderPair.class);
            joinJob.setMapOutputValueClass(Text.class);
            joinJob.setOutputKeyClass(Text.class);
            joinJob.setOutputValueClass(Text.class);
            joinJob.setGroupingComparatorClass(JoinGroupComparator.class);
            joinJob.setReducerClass(JoinReducer.class);
            //joinJob.setPartitionerClass(JoinPartitioner.class);
            joinJob.waitForCompletion(true) ;

        }

        if(stage<=4)
        {
            Job groupNeighbourhoodJob=new Job();

            groupNeighbourhoodJob.setJobName("group neighbourhood");
            groupNeighbourhoodJob.setJarByClass(MRDriverTask2.class);
            Path groupNeighbourhoodInput=new Path("joinOutput");
            Path groupNeighbourhoodOutput=new Path("groupNeighbourOutput");

            FileInputFormat.setInputPaths(groupNeighbourhoodJob, groupNeighbourhoodInput);
            FileOutputFormat.setOutputPath(groupNeighbourhoodJob, groupNeighbourhoodOutput);


            groupNeighbourhoodJob.setMapperClass(GroupNeighbourMapper.class);
            groupNeighbourhoodJob.setReducerClass(NeighbourReducer.class);
            groupNeighbourhoodJob.setNumReduceTasks(1);
            groupNeighbourhoodJob.setOutputKeyClass(Text.class);
            groupNeighbourhoodJob.setOutputValueClass(Text.class);


            groupNeighbourhoodJob.waitForCompletion(true);




        }

        if(stage<=5)
        {
            Job groupLocalityJob=new Job();

            groupLocalityJob.setJobName("Group locality");
            groupLocalityJob.setJarByClass(MRDriverTask2.class);
            Path groupLocalityInput=new Path("groupNeighbourOutput");
            Path groupLocalityOutput=new Path("groupLocalityOutput");

            FileInputFormat.setInputPaths(groupLocalityJob,groupLocalityInput);
            FileOutputFormat.setOutputPath(groupLocalityJob, groupLocalityOutput);


            groupLocalityJob.setMapperClass(GroupLocalityMapper.class);
            groupLocalityJob.setReducerClass(CountAndMaxReducer.class);
            groupLocalityJob.setNumReduceTasks(1);
            groupLocalityJob.setOutputKeyClass(Text.class);
            groupLocalityJob.setOutputValueClass(Text.class);


            groupLocalityJob.waitForCompletion(true);


        }

        if (stage<=6)
        {
            Job top10LocalityJob=new Job();

            top10LocalityJob.setJobName("Top 10 locality");
            top10LocalityJob.setJarByClass(MRDriverTask2.class);
            Path top10LocalityInput=new Path("groupLocalityOutput");
            Path top10LocalityOutput=new Path("finalOutput");

            FileInputFormat.setInputPaths(top10LocalityJob, top10LocalityInput);
            FileOutputFormat.setOutputPath(top10LocalityJob, top10LocalityOutput);


            top10LocalityJob.setMapperClass(Top10LocalityMapper.class);
            top10LocalityJob.setReducerClass(Top10CountryReducer.class);
            top10LocalityJob.setNumReduceTasks(1);
            top10LocalityJob.setMapOutputKeyClass(CountryLocalityCountPair.class);
            top10LocalityJob.setMapOutputValueClass(Text.class);
            top10LocalityJob.setOutputKeyClass(Text.class);
            top10LocalityJob.setOutputValueClass(Text.class);
            top10LocalityJob.setGroupingComparatorClass(CountryGroupComparator.class);

            top10LocalityJob.waitForCompletion(true);


        }

    }


}
