package muei.riws.tropes.indexer;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class IndexBuilder {
    static final String DOC_TYPE = "_doc";
    static final String DOC_PROPERTIES = "properties";
    
    public static void createIndex(String indexName, Map<String,String> indexFields) throws IOException {
        // New index request will be sent to ES service.
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        
        // A builder which contains new index mapping information.
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject(DOC_TYPE);
            {
                builder.startObject(DOC_PROPERTIES);
                {
                    for (Map.Entry<String, String> indexField : indexFields.entrySet()) {
                        //TODO
                    }
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        request.mapping("_doc", builder);
        
    }

}
