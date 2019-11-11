package muei.riws.tropes.indexer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class TropeIndexer {
    
	private static final String INDEX_NAME = "tropes";
	
	private static class Trope {
		public String Name;
		public String Content;
		public String Url;
		public Map<String, List<String>> Media = new HashMap<String, List<String>>();
		
		public Trope(String name, String content, String url, Map<String, List<String>> media) {
			Name = name;
			Content = content;
			Url = url;
			Media = media;
		}
		// TODO laconic, related tropes
	}
	
	private static XContentBuilder createTropeMapping() throws IOException {
		// A builder which contains new index mapping information.
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
        	.startObject("_doc")
        		.startObject("properties")
        			.startObject("name").field("type", "text").endObject()
                    .startObject("content").field("type", "text").endObject()
                    .startObject("laconic").field("type", "text").endObject()
            		.startObject("url").field("type", "text").endObject()
            		.startObject("related_tropes").field("type", "text").endObject()
            		.startObject("media")
            			.startObject("properties")
	                    	.startObject("media_type").field("type", "text").endObject()
	                    	.startObject("media_urls").field("type", "text").endObject()
    	                .endObject()	                
	                .endObject()
            	.endObject()
        	.endObject()
    	.endObject();
        
        return builder;
	}
    
    private static TransportClient createClient() throws IOException {
        // ES Client settings
        Settings settings = Settings.builder()
                .put("node.name", "kiiKFUc").build();
        
        // Create a transport client
        // Port 9300 for node-to-node communication
        @SuppressWarnings("resource")
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));	
        
        return client;
    }
	
    private static void createTropeIndex() throws IOException {
        // New index request will be sent to ES service.
        CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME);
        // TODO comprobar si el indice existe y eliminarlo
        XContentBuilder builder = createTropeMapping();
        
        request.mapping("_doc", builder);
        
        TransportClient client = createClient();
        client.admin().indices().create(request);
        client.close();
        
    }
    
    private static void indexTropeDocument(Trope trope) throws IOException {

    	TransportClient client = createClient();
    	XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
	    	.field("name", trope.Name)
	    	.field("content", trope.Content)
	    	.field("url", trope.Url)
    		.startArray("media");
		    		for(String mediaType : trope.Media.keySet()) {
		    			contentBuilder
			    		.startObject()
		    			.field("media_type", mediaType)
    					.field("media_urls", trope.Media.get(mediaType))
    					.endObject();
		        	}
		    		
		contentBuilder
    		.endArray()
    		.endObject();
    	
        IndexResponse response = client
        		.prepareIndex(INDEX_NAME, "_doc")
        		.setSource(contentBuilder)
        		.get();

        System.out.println("We have indexed the trope " + trope.Name + " in the index " + response.getIndex());

        client.close();
    }
    
    
    private static Map<String, List<String>> parseJsonMedia(JSONArray mediaLinks) {
    	//String mediaType = (String) mediaContent.get(key)
    	Map<String, List<String>> mediaList = new HashMap<String, List<String>>();
    	
    	Pattern mediaPattern = Pattern.compile("php/(.*?)/");
    	
    	for(Object urlObj : mediaLinks) {
    		String url = (String) urlObj;
    		// Extract the media type from the url (format "...php/[Media type]/...(
    		// TODO enum de medios? (para alguna funcionalidad)
    		Matcher m = mediaPattern.matcher(url);
    		m.find();
    		String mediaType = m.group(1);
    		if(mediaList.containsKey(mediaType)) {
    			mediaList.get(mediaType).add(url);
    		} else {
    			List<String> urls = new ArrayList<String>();
    			urls.add(url);
    			mediaList.put(mediaType, urls);
    		}
    	}
    	
    	return mediaList;
    }
    
	@SuppressWarnings("unchecked")
	private static Trope parseJsonTrope(JSONObject jsonContent) {

		//Get trope name
		String tropeName = (String) jsonContent.get("title");
		
		//Get trope content
		String tropeContent = (String) jsonContent.get("content");	

		
		//Get trope url
		String tropeUrl = (String) jsonContent.get("url");	

		//Get list of media links and convert it to a list of media
		JSONArray mediaLinks = (JSONArray) jsonContent.get("media_links");
		Map<String, List<String>> tropeMedia = parseJsonMedia(mediaLinks);
		
		Trope t = new Trope(tropeName, tropeContent, tropeUrl, tropeMedia);
		
		return t;
	}
    
	private static List<JSONObject> getJsonData(final File folder) {

		List<JSONObject> tropeData = new ArrayList<JSONObject>();
		
    	//JSON parser object to parse read file
		JSONParser jsonParser = new JSONParser();
		
	    for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	            tropeData.addAll(getJsonData(fileEntry));
	        } else {
	        	if(!fileEntry.getName().contains("laconic")) { // TODO
		        	try (FileReader reader = new FileReader(fileEntry)) // TODO comprobar consumo de memoria
		    		{
		    			//Read JSON file
		                Object obj = jsonParser.parse(reader);
	
		                JSONObject trope = (JSONObject) obj;
		                
		                tropeData.add(trope);
		                
		    		} catch (FileNotFoundException e) {
		                e.printStackTrace();
		                System.out.println("File " + fileEntry.getName());
		            } catch (IOException e) {
		                e.printStackTrace();
		                System.out.println("File " + fileEntry.getName());
		            } catch (ParseException e) {
		                e.printStackTrace();
		                System.out.println("File " + fileEntry.getName());
		            }
	        	}
	        }
	    }
	    
	    return tropeData;
	    
	}
	
    public static void main(String args[]) throws IOException {

    	
    	createTropeIndex();

    	String dataFolder = "C:\\Users\\Diego\\Documents\\Universidad\\MUEI\\Recuperacion de la informacion y Web Semantica\\Practica RI\\riws-crawler\\crawler\\data";
    	final File folder = new File(dataFolder);
    	List<JSONObject> tropeData = getJsonData(folder);
    	
    	// TODO eliminar variable debug
    	int i = 0;
    	int imax = 2;
    	for(JSONObject data : tropeData) {
    		Trope t = parseJsonTrope(data);
    		try {
    			indexTropeDocument(t);
    		} catch (IOException e) {
    			System.out.println("Could not index trope " + t.Name);
    			e.printStackTrace();
    		}
    		i++;
    		if(i >= imax)
    			break;
    	}
    }
    

}
