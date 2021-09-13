package rcache.executor;

import lombok.AllArgsConstructor;
import lombok.Data;
import rcache.response.Response;

@Data
@AllArgsConstructor
public class ResultSet {
    private boolean isConnectionHold;
    private Response response;
}
