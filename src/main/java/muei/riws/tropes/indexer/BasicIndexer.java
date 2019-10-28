package muei.riws.tropes.indexer;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class BasicIndexer {
    
    public BasicIndexer() {
        
    }
    
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
    
    public static void indexExampleDocument() {

    }
    

}
