package rcache.executor;

import lombok.Data;
import rcache.response.Response;

@Data
public class ResultSet<V> {
    private boolean isConnectionHold;
    private Response response;
}
