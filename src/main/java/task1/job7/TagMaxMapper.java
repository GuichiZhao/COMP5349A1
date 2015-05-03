package task1.job7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by guichi on 18/04/2015.
 */
public class TagMaxMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
    static Integer [] rankList=new Integer[51];


    public TagMaxMapper() {
        super();
        for (int i=0;i<51;i++)
        {
            rankList[i]=0;
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line =value.toString();
        String[] parts=line.split("\t");
        int count=Integer.parseInt(parts[0]);
        int rank =Integer.parseInt( parts[1].split("\\$")[0]);

        //System.out.println("\n\n\n\n\n\n\n\n\n"+rank+"\n"+rankList[rank]);

        if(rankList[rank]<10)
        {
            context.write(new IntWritable(count),new Text(parts[1]));
            rankList[rank]++;
        }



    }
}
