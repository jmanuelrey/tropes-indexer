package muei.riws.tropes.indexer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class TropeIndexer {
    
	private static final String INDEX_NAME = "tropes";
	private static final String NODE_NAME = "kiiKFUc";
	
	public static class Pair<T, U> {         
	    public final T trope;
	    public final U laconic;

	    public Pair(T t, U u) {         
	        this.trope= t;
	        this.laconic= u;
	     }
	 }
	
	private static class Trope {
		public String Name;
		public String Content;
		public String Url;
		public String Laconic;
		public List<String> RelatedTropes;
		public int RelatedTropesCount;
		public Map<String, List<String>> Media = new HashMap<String, List<String>>();
		public int MediaTypeCount;
		public int MediaUrlsCount;
		
		public Trope(String name, String content, String url, String laconic, List<String> relatedTropos, Map<String, List<String>> media) {
			Name = name;
			Content = content;
			Url = url;
			Laconic = laconic;
			RelatedTropes = relatedTropos;	
			RelatedTropesCount = RelatedTropes.size();
			Media = media;
			MediaTypeCount = Media.size();
			MediaUrlsCount = 0;
			for(List<String> urls : Media.values()) {
				MediaUrlsCount += urls.size();
			}
		}
	}
	
	private static XContentBuilder createTropeMapping() throws IOException {
		// A builder which contains new index mapping information.
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
        	.startObject("_doc")
        		.startObject("properties")
        			.startObject("name")
        				.field("type", "text")
        				.startObject("fields")
        					.startObject("raw")
        						.field("type", "keyword")
        					.endObject()
    					.endObject()
    				.endObject()
                    .startObject("content")
                    	.field("type", "text")
                	.endObject()
                    .startObject("laconic")
                    	.field("type", "text")
                	.endObject()
            		.startObject("url")
            			.field("type", "text")
        			.endObject()
            		.startObject("related_tropes")
            			.field("type", "text")
        			.endObject()
        			.startObject("related_tropes_count")
        				.field("type", "integer")
        			.endObject()
            		.startObject("media")
    					.field("type", "nested")
            			.startObject("properties")
	                    	.startObject("media_type")
	                    		.field("type", "text")
                    		.endObject()
	                    	.startObject("media_urls")
	                    		.field("type", "text")
                    		.endObject()
    	                .endObject()	                
	                .endObject()
            		.startObject("media_type_count")
	        			.field("type", "long")
	        		.endObject()
	        		.startObject("media_urls_count")
	        			.field("type", "long")
	        		.endObject()
            	.endObject()
        	.endObject()
    	.endObject();
        
        return builder;
	}
    
    private static TransportClient createClient() throws IOException {
        // ES Client settings
        Settings settings = Settings.builder()
                .put("node.name", NODE_NAME).build();
        
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
	    	.field("laconic", trope.Laconic)
	    	.field("related_tropes", trope.RelatedTropes)
	    	.field("related_tropes_count", trope.RelatedTropesCount)
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
			.field("media_type_count", trope.MediaTypeCount)
			.field("media_urls_count", trope.MediaUrlsCount)
    		.endObject();
    	
        IndexResponse response = client
        		.prepareIndex(INDEX_NAME, "_doc")
        		.setSource(contentBuilder)
        		.get();

        System.out.println("We have indexed the trope " + trope.Name + " in the index " + response.getIndex());

        client.close();
    }
    
    private static List<String> parseJsonRelatedTropes(JSONArray nonMediaLinks) {
    	
    	List<String> relatedTropes = new ArrayList<String>();
    	
    	Pattern relatedPattern = Pattern.compile("php/Main/*");
    	
    	for(Object urlObj : nonMediaLinks) {
    		String url = (String) urlObj;
    		// Extract the related tropes from the url (format "...php/Main/[Trope])
    		// TODO comprobar que la url es realmente un tropo
    		// NOTA: aunque no lo fuese no afectar�a especialmente a las estad�sticas, ya que todos los tropos tendr�an m�s o menos los mismos
    		// enlaces "err�neos".
    		Matcher m = relatedPattern.matcher(url);
    		if(m.find())
    			relatedTropes.add(url);
    	}
    	
    	return relatedTropes;
    	
    }
    
    private static Map<String, List<String>> parseJsonMedia(JSONArray mediaLinks) {
    	//String mediaType = (String) mediaContent.get(key)
    	Map<String, List<String>> mediaList = new HashMap<String, List<String>>();
    	
    	Pattern mediaPattern = Pattern.compile("php/(.*?)/");
    	
    	for(Object urlObj : mediaLinks) {
    		String url = (String) urlObj;
    		// Extract the media type from the url (format "...php/[Media type]/...)
    		Matcher m = mediaPattern.matcher(url);
    		if(m.find()) {
	    		String mediaType = m.group(1);
	    		if(mediaList.containsKey(mediaType)) {
	    			mediaList.get(mediaType).add(url);
	    		} else {
	    			List<String> urls = new ArrayList<String>();
	    			urls.add(url);
	    			mediaList.put(mediaType, urls);
	    		}
    		}
    	}
    	
    	return mediaList;
    }
    
    private static Trope parseJsonLaconic(JSONObject jsonContent) {
		// Get trope name
		String tropeName = (String) jsonContent.get("title");
		
		// Get laconic content
		String laconic = (String) jsonContent.get("content");

    	Trope trope = new Trope(tropeName, "", "", laconic, new ArrayList<String>(), new HashMap<String, List<String>>());
    	
    	return trope;
    }
    
	@SuppressWarnings("unchecked")
	private static Trope parseJsonTrope(JSONObject jsonContent) {

		// Get trope name
		String tropeName = (String) jsonContent.get("title");
		System.out.println(tropeName);
		
		// Get trope content
		String tropeContent = (String) jsonContent.get("content");	

		// Get trope url
		String tropeUrl = (String) jsonContent.get("url");	

		// Get laconic content
		//String laconic = (String) jsonContent.get("laconic");
		String laconic = "";
		
		// Get related tropes
		JSONArray nonMediaLinks = (JSONArray) jsonContent.get("non_media_links");
		List<String> relatedTropes = parseJsonRelatedTropes(nonMediaLinks);
		
		// Get list of media links and convert it to a list of media
		JSONArray mediaLinks = (JSONArray) jsonContent.get("media_links");
		Map<String, List<String>> tropeMedia = parseJsonMedia(mediaLinks);
		
		return new Trope(tropeName, tropeContent, tropeUrl, laconic, relatedTropes, tropeMedia);
	}
    
	private static Pair<List<JSONObject>, List<JSONObject>> getJsonData(final File folder) {

		Pair<List<JSONObject>, List<JSONObject>> allData = new Pair<List<JSONObject>, List<JSONObject>>(new ArrayList<JSONObject>(), new ArrayList<JSONObject>());
		
    	//JSON parser object to parse read file
		JSONParser jsonParser = new JSONParser();
		
	    for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	        	Pair<List<JSONObject>, List<JSONObject>> data = getJsonData(fileEntry);
	        	allData.trope.addAll(data.trope);
	        	allData.laconic.addAll(data.laconic);
	        } else {
	        	try (FileReader reader = new FileReader(fileEntry)) // TODO comprobar consumo de memoria
	    		{
	    			//Read JSON file
	                Object obj = jsonParser.parse(reader);
	                
	                JSONObject trope = (JSONObject) obj;
	                
	                if(fileEntry.getName().contains("laconic")) {
	                	allData.laconic.add(trope);
	                } else {
		                allData.trope.add(trope);
	                }
	                
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
	    
	    return allData;
	    
	}
	
    public static void main(String args[]) throws IOException, URISyntaxException {

    	
    	createTropeIndex();

    	
    	URL res = TropeIndexer.class.getResource("/data");
    	final File folder = Paths.get(res.toURI()).toFile();

    	Pair<List<JSONObject>, List<JSONObject>> allData = getJsonData(folder);
    	List<JSONObject> tropeData = allData.trope;
    	List<JSONObject> laconicData = allData.laconic;
    	
    	List<Trope> tropes = new ArrayList<Trope>();
    	
    	for(JSONObject data : tropeData) {
    		Trope trope = parseJsonTrope(data);
    		tropes.add(trope);
    	}
    	
    	for(JSONObject data : laconicData) {
    		Trope laconic = parseJsonLaconic(data);
    		for(Trope t : tropes) {
    			if(t.Name == laconic.Name) {
    				tropes.get(tropes.indexOf(t)).Laconic = laconic.Content;
    				break;
    			}
    		}
    	}

    	// TODO eliminar variable debug
    	int i = 0;
    	int imax = 5;
    	for(Trope t : tropes) {
    		if(i >= imax)
    			break;
    		try {
    			indexTropeDocument(t);
    		} catch (IOException e) {
    			System.out.println("Could not index trope " + t.Name);
    			e.printStackTrace();
    		}
    		i++;
    	}
    }
    

}
