package edu.uci.ics.hcheng10.service.gateway.threadpool;


import com.braintreepayments.http.exceptions.SerializeException;
import com.braintreepayments.http.serializer.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.hcheng10.service.gateway.GatewayService;
import edu.uci.ics.hcheng10.service.gateway.logger.ServiceLogger;
import org.glassfish.jersey.jackson.JacksonFeature;

import javax.annotation.Nullable;
import javax.ws.rs.client.*;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ClientRequest {
    /* User Information */
    private String email;
    private String session_id;
    private String transaction_id;
    /* Target Service and Endpoint */
    private String URI;
    private String endpoint;
    private HTTPMethod method;

    /*
     * So before when we wanted to get the request body
     * we would grab it as a String (String jsonText).
     *
     * The Gateway however does not need to see the body
     * but simply needs to pass it. So we save ourselves some
     * time and overhead by grabbing the request as a byte array
     * (byte[] jsonBytes).
     *
     * This way we can just act as a
     * messenger and just pass along the bytes to the target
     * service and it will do the rest.
     *
     * for example:
     *
     * where we used to do this:
     *
     *     @Path("hello")
     *     ...ect
     *     public Response hello(String jsonString) {
     *         ...ect
     *     }
     *
     * do:
     *
     *     @Path("hello")
     *     ...ect
     *     public Response hello(byte[] jsonBytes) {
     *         ...ect
     *     }
     *
     */
    private byte[] requestBytes;
    private HashMap<String, Object> queries;
    private HttpHeaders headers;
    public String getTransaction_id() {
        return transaction_id;
    }

    public ClientRequest(String transaction_id, String URI, String endpoint, HTTPMethod method, @Nullable byte[] requestBytes, @Nullable HashMap<String, Object> queries, @Nullable HttpHeaders headers) {
        this.transaction_id = transaction_id;
        this.URI = URI;
        this.endpoint = endpoint;
        this.method = method;
        this.requestBytes = requestBytes;
        this.queries = queries;
        this.headers = headers;
    }

    private void sendRequestForBilling() {
        ServiceLogger.LOGGER.info("Building client...");
        Client client = ClientBuilder.newClient();
        client.register(JacksonFeature.class);

        ServiceLogger.LOGGER.info("Building WebTarget "+this.URI+this.endpoint);
        WebTarget webTarget = client.target(this.URI).path(this.endpoint);

        ServiceLogger.LOGGER.info("Starting invocation builder...");


        ServiceLogger.LOGGER.info(String.format("Sending %s request to %s, endpoint %s", this.method.toString(), this.URI, this.endpoint));
        Response response;
        if (this.method.toString().equals("POST")) {
            Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON)
                    .header("email", this.email)
                    .header("session_id", this.session_id)
                    .header("transaction_id", this.transaction_id);
            response = invocationBuilder.post(Entity.entity(this.requestBytes, MediaType.APPLICATION_JSON));
        } else if (this.method.toString().equals("GET")) {
            for (Map.Entry<String, Object> entry : queries.entrySet()) {
                webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
            }
            Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON)
                    .header("email", this.email)
                    .header("session_id", this.session_id)
                    .header("transaction_id", this.transaction_id);
            response = invocationBuilder.get();
        } else {
            ServiceLogger.LOGGER.info("Detected other HttpRequest Type: " + this.method.toString());
            return;
        }
        ServiceLogger.LOGGER.info("Request sent.");
        ServiceLogger.LOGGER.info(String.format("Received status %s for user with transaction_id %s", response.getStatus(), this.transaction_id));
        try {
            String email = this.headers.getHeaderString("email");
            String session_id = this.headers.getHeaderString("session_id");
            this.insertIntoResponse(this.transaction_id, response.readEntity(String.class), response.getStatus(), this.headers.getHeaderString("email"), this.headers.getHeaderString("session_id"));
        } catch (Exception e) {e.printStackTrace();}


    }

    private void sendRequestForIdm() {
        ServiceLogger.LOGGER.info("Building client...");
        Client client = ClientBuilder.newClient();
        client.register(JacksonFeature.class);

        ServiceLogger.LOGGER.info("Building WebTarget "+this.URI+this.endpoint);
        WebTarget webTarget = client.target(this.URI).path(this.endpoint);

        ServiceLogger.LOGGER.info("Starting invocation builder...");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON)
                .header("email", this.email)
                .header("session_id", this.session_id)
                .header("transaction_id", this.transaction_id);

        ServiceLogger.LOGGER.info(String.format("Sending %s request to %s, endpoint %s", this.method.toString(), this.URI, this.endpoint));
        Response response;
        if (this.method.toString().equals("POST")) {
            response = invocationBuilder.post(Entity.entity(this.requestBytes, MediaType.APPLICATION_JSON));
        } else if (this.method.toString().equals("GET")) {
            response = invocationBuilder.get();
        } else {
            ServiceLogger.LOGGER.info("Detected other HttpRequest Type: " + this.method.toString());
            return;
        }
        ServiceLogger.LOGGER.info("Request sent.");
        ServiceLogger.LOGGER.info(String.format("Received status %s for user with transaction_id %s", response.getStatus(), this.transaction_id));
        String res = response.hasEntity() ? response.readEntity(String.class) : "";
            try {
                ObjectMapper mapper = new ObjectMapper();
                switch (this.endpoint) {
                    case "/register":
                        this.insertIntoResponse(this.transaction_id, res, response.getStatus(), null, null);
                        return;
                    case "/login": {
                        JsonNode rootNode = mapper.readTree(res);
                        if (rootNode.get("resultCode").asText().equals("120"))
                            this.insertIntoResponse(this.transaction_id, res, response.getStatus(), null, rootNode.get("session_id").asText());
                        else this.insertIntoResponse(this.transaction_id, res, response.getStatus(), null, null);
                        break;
                    }
                    case "/session": {
                        JsonNode rootNode = mapper.readTree(res);
                        this.insertIntoResponse(this.transaction_id, res, response.getStatus(), null, rootNode.get("session_id").asText());
                        break;
                    }
                    case "/privilege":
                        this.insertIntoResponse(this.transaction_id, res, response.getStatus(), null, null);
                        break;
                }
            } catch (Exception e) {e.printStackTrace();}

    }

    private void sendRequestForMovies() {
        ServiceLogger.LOGGER.info("Building client...");
        Client client = ClientBuilder.newClient();
        client.register(JacksonFeature.class);
        ServiceLogger.LOGGER.info("Building WebTarget "+this.URI+this.endpoint);
        WebTarget webTarget = client.target(this.URI).path(this.endpoint);
        Response response = null;
        if (this.method.toString().equals("GET") && this.queries != null) {
            ServiceLogger.LOGGER.info("Starting invocation builder...");
            if (!this.endpoint.startsWith("/get")) {
                for (Map.Entry<String, Object> entry : queries.entrySet()) {
                    if (!entry.getKey().equals("movie_id") && !entry.getKey().equals("person_id") && !entry.getKey().equals("phrase") && entry.getValue() != null && !entry.getValue().equals(0)) {
                        webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
                    }
                }
            }
            if (this.endpoint.startsWith("/browse")) webTarget = webTarget.path(queries.get("phrase").toString());
            if (this.endpoint.startsWith("/get")) webTarget = webTarget.path(queries.get("movie_id").toString());
            if (this.endpoint.startsWith("/people/get")) webTarget = webTarget.path(queries.get("person_id").toString());
            Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
            ServiceLogger.LOGGER.info(webTarget.toString());
            response = invocationBuilder.get();
        } else if (this.method.toString().equals("POST")) {
            ServiceLogger.LOGGER.info("Starting invocation builder...");
            Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
            response = invocationBuilder.post(Entity.entity(this.requestBytes, MediaType.APPLICATION_JSON));
            ServiceLogger.LOGGER.info(webTarget.toString());
        }
        ServiceLogger.LOGGER.info(String.format("Sending %s request to %s, endpoint %s", this.method.toString(), this.URI, this.endpoint));

        this.insertIntoResponse(this.transaction_id, response.readEntity(String.class), response.getStatus(), this.headers.getHeaderString("email"), this.headers.getHeaderString("session_id"));
    }

    public void sendRequest() {
        if (this.URI.equals(GatewayService.getIdmConfigs().getScheme()+GatewayService.getIdmConfigs().getHostName()+":"+GatewayService.getIdmConfigs().getPort()+GatewayService.getIdmConfigs().getPath())) {
            this.sendRequestForIdm();
        } else if (this.URI.equals(GatewayService.getMoviesConfigs().getScheme()+GatewayService.getMoviesConfigs().getHostName()+":"+GatewayService.getMoviesConfigs().getPort()+GatewayService.getMoviesConfigs().getPath())) {
            this.sendRequestForMovies();
        } else if (this.URI.equals(GatewayService.getBillingConfigs().getScheme()+GatewayService.getBillingConfigs().getHostName()+":"+GatewayService.getBillingConfigs().getPort()+GatewayService.getBillingConfigs().getPath())) {
            this.sendRequestForBilling();
        }
    }

    private void insertIntoResponse(String transaction_id, String res, Integer status, @Nullable String email, @Nullable String session_id) {
        Connection con = GatewayService.getConnectionPoolManager().requestCon();
        try {
            String query = "INSERT INTO responses(transaction_id, email, session_id, response, http_status) VALUES (?,?,?,?,?)";
            PreparedStatement ps = con.prepareStatement(query);
            ps.setString(1, transaction_id);
            ps.setString(2, email);
            ps.setString(3, session_id);
            ps.setString(4, res);
            ps.setInt(5, status);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            GatewayService.getConnectionPoolManager().releaseCon(con);
        }
    }

}

