package org.skyline.tools.es.plugin;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;

/**
 * @author Sean Liu
 * @date 2019-11-20
 */
public class Hive2ESOfflineHandler extends BaseRestHandler {

  @Inject
  public Hive2ESOfflineHandler(Settings settings,
      RestController controller, Client client) {
    super(settings, controller, client);
    controller.registerHandler(Method.POST, "_hive2es", this);
  }

  @Override
  protected void handleRequest(RestRequest request, RestChannel channel, Client client)
      throws Exception {
    String dataPath = settings.get("path.data");
    channel.sendResponse(new BytesRestResponse(RestStatus.OK, settings.toDelimitedString('\n')));
  }
}
