package hadoop;

import hadoop.WordCount.IntSumReducer;
import hadoop.WordCount.TokenizerMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;

public class GeoLocationExample {


	/*public static class GeoLocationMapper extends MapReduceBase  implements
    Mapper<LongWritable, Text, Text, Text> { */
		
	public static class GeoLocationMapper extends Mapper<LongWritable, Text, Text, Text> {	
	
		public static String GEO_RSS_URI = "http://www.georss.org/georss/point";

        private Text geoLocationKey = new Text();
        private Text geoLocationName = new Text();
        
        
        public void loadData(String dataRow, OutputCollector<Text, Text> outputCollector) throws Exception {
            Model model = null;
            RDFDataMgr.read(model, new ByteArrayInputStream(dataRow.getBytes(StandardCharsets.UTF_8)),Lang.NT);

            String queryString = "SELECT ?name WHERE { ?name <http://dbpedia.org/property/wikilink> ?wikilink . }";
            Query query = QueryFactory.create(queryString, Syntax.syntaxARQ);
    		QueryExecution qexec = QueryExecutionFactory.create(query, model) ;
            
            ResultSet rs = qexec.execSelect();
            
            while (rs.hasNext()) {
            	
	            	QuerySolution soln;
					soln = rs.nextSolution();
					String articleName = "";
					if(soln.get("name") != null){
						
					String queryS = "SELECT ?name ?point WHERE { ?name <http://www.georss.org/georss/point> ?point . }";	
			        Query q = QueryFactory.create(queryS, Syntax.syntaxARQ);
		    		QueryExecution qexe = QueryExecutionFactory.sparqlService("http://dbpedia.org/sparql", q);
		    		ResultSet results = qexe.execSelect();
		    		
		    		while (results.hasNext()) {
		    			QuerySolution soln1;
		    			soln1= results.nextSolution();
		    			if (soln1.get("point")!=null) {
		    				
	                        StringTokenizer st = new StringTokenizer(soln1.get("point").asLiteral().toString(), " ");
	                        String strLat = st.nextToken();
	                        String strLong = st.nextToken();
	                        double lat = Double.parseDouble(strLat);
	                        double lang = Double.parseDouble(strLong);
	                        long roundedLat = Math.round(lat);
	                        long roundedLong = Math.round(lang);
	                        String locationKey = "(" + String.valueOf(roundedLat) + ","
	                                        + String.valueOf(roundedLong) + ")";
	                        String locationName = URLDecoder.decode(articleName, "UTF-8");
	                        locationName = locationName.replace("_", " ");
	                        geoLocationKey.set(locationKey);
	                        geoLocationName.set(locationName);
	                        outputCollector.collect(geoLocationKey, geoLocationName);
		    			}
		    		}
				}
            }
        }
        
        //@Override
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, Text> outputCollector, Reporter reporter)
                        throws Exception {

                String dataRow = value.toString();
                
                loadData(dataRow,outputCollector);
                
                // since these are tab seperated files lets tokenize on tab
                StringTokenizer dataTokenizer = new StringTokenizer(dataRow, "\t");
                String articleName = dataTokenizer.nextToken();
                String pointType = dataTokenizer.nextToken();
                String geoPoint = dataTokenizer.nextToken();
                // we know that this data row is a GEO RSS type point.
                if (GEO_RSS_URI.equals(pointType)) {
                        // now we process the GEO point data.
                        StringTokenizer st = new StringTokenizer(geoPoint, " ");
                        String strLat = st.nextToken();
                        String strLong = st.nextToken();
                        double lat = Double.parseDouble(strLat);
                        double lang = Double.parseDouble(strLong);
                        long roundedLat = Math.round(lat);
                        long roundedLong = Math.round(lang);
                        String locationKey = "(" + String.valueOf(roundedLat) + ","
                                        + String.valueOf(roundedLong) + ")";
                        String locationName = URLDecoder.decode(articleName, "UTF-8");
                        locationName = locationName.replace("_", " ");
                        geoLocationKey.set(locationKey);
                        geoLocationName.set(locationName);
                        outputCollector.collect(geoLocationKey, geoLocationName);
                }
        }
	}
	
	
	public static class GeoLocationReducer extends Reducer<Text, Text, Text, Text> {
	 //GeoLocationReducer extends MapReduceBase implements
     //Reducer<Text, Text, Text, Text> {
			
	        private Text outputKey = new Text();
	        private Text outputValue = new Text();

	        //@Override
	        public void reduce(Text anagramKey, Iterator<Text> anagramValues,
	                        OutputCollector<Text, Text> results, Reporter reporter)
	                        throws IOException {
	                // in this case the reducer just creates a list so that the data can
	                // used later
	                String outputText = "";
	                while (anagramValues.hasNext()) {
	                        Text locationName = anagramValues.next();
	                        outputText = outputText + locationName.toString() + " ,";
	                }
	                outputKey.set(anagramKey.toString());
	                outputValue.set(outputText);
	                results.collect(outputKey, outputValue);
	        }
	
	}
	
    public static void main(String[] args) throws Exception {

    	
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "geolocationgroup");
		job.setJarByClass(GeoLocationExample.class);
		job.setMapperClass(GeoLocationMapper.class);
		//job.setCombinerClass(GeoLocationReducer.class);
		job.setReducerClass(GeoLocationReducer.class);
		job.setOutputKeyClass(Text.class);
		
		//job.setOutputValueClass(IntWritable.class);
		
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
    	
    	
    	
    	
/*        JobConf conf = new JobConf(GeoLocationExample.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        Job job = Job.getInstance(conf, "geolocationgroup");
        
        //conf.setJobName("geolocationgroup");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(GeoLocationMapper.class);
        // conf.setCombinerClass(AnagramReducer.class);
        job.setReducerClass(GeoLocationReducer.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        JobClient.runJob(conf);*/
        

}

}
