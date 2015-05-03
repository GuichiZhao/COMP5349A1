package task2.job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by guichi on 20/04/2015.
 */
public class PlaceFilterMapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String [] parts=line.split("\t");
        if (parts.length==7)
        {
            String url=parts[6];
            String []url_part=url.split("/");


            String countryName="/"+url_part[1];
            String localityName="";
            String neighbourName="";
            int type_id=Integer.parseInt(parts[5]);
            if (type_id==7)
            {
                localityName=parts[6];
                neighbourName="NULL";

                context.write(new Text(parts[0]),new Text(countryName+"\t"+localityName+"\t"+neighbourName));
            }
            if(type_id==22)
            {
                neighbourName=parts[6];
                //String[] neighbourParts=neighbourName.split("\t");
                StringBuffer localityBuffer=new StringBuffer();
                for (int i=0;i<url_part.length-1;i++)
                {
                    localityBuffer.append(url_part[i]);
                    localityBuffer.append("/");
                }
                localityBuffer.deleteCharAt(localityBuffer.lastIndexOf("/"));

                localityName=localityBuffer.toString();


                context.write(new Text(parts[0]),new Text(countryName+"\t"+localityName+"\t"+neighbourName));

            }



        }
    }
}
