package muei.riws.tropes.indexer;

import java.io.IOException;
import java.net.InetAddress;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class BasicIndexer {
    
    public static void createExampleIndex() throws IOException {
        // New index request will be sent to ES service.
        CreateIndexRequest request = new CreateIndexRequest("twitter");
        
        // A builder which contains new index mapping information.
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("_doc");
            {
                builder.startObject("properties");
                {
                    builder.startObject("message");
                    {
                        builder.field("type", "text");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        request.mapping("_doc", builder);
        
    }
    
    public static void indexExampleDocument() throws IOException {
        // ES Client settings
        Settings settings = Settings.builder()
                .put("node.name", "kiiKFUc").build();
        
        // Create a transport client
        // Port 9300 for node-to-node communication
        @SuppressWarnings("resource")
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
        
        IndexResponse response = client.prepareIndex("twitter", "_doc", "1")
                .setSource(XContentFactory.jsonBuilder()
                            .startObject()
                                .field("message", "trying le Diegos")
                            .endObject()
                          )
                .get();
        
        System.out.println("We have indexed a document in the index " + response.getIndex());
        client.close();

    }
    
    public static void main(String args[]) throws IOException {
        createExampleIndex();
        indexExampleDocument();
        
        
    }
    

}
