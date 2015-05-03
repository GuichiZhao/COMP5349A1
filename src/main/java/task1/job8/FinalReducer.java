package task1.job8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by guichi on 18/04/2015.
 */
public class FinalReducer extends Reducer<IntWritable,Text,Text,Text>{
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String url="";
        String count="";
        String tagAndCount="";

        List<TagFreq> l=new ArrayList<>();

        Iterator<Text> itr=values.iterator();
        String rank=key.toString();

        while (itr.hasNext())
        {
            String line=itr.next().toString();
            String[] parts= line.split("\t");
            url=parts[0];
            count=parts[1];
            String tmp=parts[2]+":"+parts[3]+" ";
            tagAndCount+=tmp;

            l.add(new TagFreq(Integer.parseInt(parts[3]),parts[2]));

        }

        Collections.sort(l);
        String tagFreq="";
        for (TagFreq tf:l)
        {
            tagFreq+=tf;
        }


        context.write(new Text(url+"\t"+count),new Text(tagFreq));



    }

    private class TagFreq implements Comparable<TagFreq>
    {
        private int f;
        private String t;

        public TagFreq(int f, String t) {
            this.f = f;
            this.t = t;
        }

        @Override
        public int compareTo(TagFreq o) {
            if(f!=o.f)
            {
                return new Integer(o.f).compareTo(new Integer(f));
            }
            else
            {
                return t.compareTo(o.t);
            }
        }

        @Override
        public String toString() {
            return t+":"+f+" ";
        }
    }
}
