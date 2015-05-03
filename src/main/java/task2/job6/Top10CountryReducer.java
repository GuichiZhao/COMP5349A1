package task2.job6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by guichi on 22/04/2015.
 */
public class Top10CountryReducer extends Reducer<CountryLocalityCountPair,Text,Text,Text>{
    @Override
    protected void reduce(CountryLocalityCountPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator=values.iterator();
        int i=0;
        String country="";
        ListAdaptor localityList=new ListAdaptor();
        ListAdaptor neighbourList=new ListAdaptor();

        while (iterator.hasNext()) {
            if (i < 10)
            {
                String s = iterator.next().toString();
                String[] parts=s.split("\t");
                country=key.getCountry().toString();
                int localityCount=key.getLocalityCount().get();

                String localityName=parts[0];
                String [] localityNameParts=localityName.split("/");
                String exactLocalityName=localityNameParts[localityNameParts.length-1];


                String neighbourName=parts[1];
                String [] neighbourNameParts=neighbourName.split("/");
                String exactNeighbourName=neighbourNameParts[neighbourNameParts.length-1];
                int neighbourCount=Integer.parseInt(parts[2]);

                localityList.add(exactLocalityName,localityCount);
                neighbourList.add(exactNeighbourName, neighbourCount);




                i++;
            }

            else break;
        }

        context.write(new Text(country), new Text(localityList+""+neighbourList));
    }

    class ListAdaptor
    {
        List<String> l=new ArrayList<>();
        void add(String name,int count)
        {
            l.add(new String(name+":"+count));
        }

        @Override
        public String toString() {
            String result="";
            for (String s:l)
            {
                result+=s;
                result+=" ";
            }
            return result;
        }
    }
}
