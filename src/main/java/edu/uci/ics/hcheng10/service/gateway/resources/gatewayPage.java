package edu.uci.ics.hcheng10.service.gateway.resources;

import edu.uci.ics.hcheng10.service.gateway.GatewayService;
import edu.uci.ics.hcheng10.service.gateway.logger.ServiceLogger;
import edu.uci.ics.hcheng10.service.gateway.threadpool.ClientRequest;
import edu.uci.ics.hcheng10.service.gateway.threadpool.HTTPMethod;
import edu.uci.ics.hcheng10.service.gateway.transaction.TransactionGenerator;
import org.glassfish.jersey.jackson.JacksonFeature;

import javax.annotation.Nullable;
import javax.ws.rs.*;
import javax.ws.rs.client.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

@Path("")
public class gatewayPage {

    @Path("idm/{endpoint}")
    @POST
    @Produces()
    public Response idmRequest(@Context HttpHeaders header, @PathParam("endpoint") String endpoint, byte[] args) {
        String tid = TransactionGenerator.generate();
        GatewayService.getThreadPool().putRequest(new ClientRequest(tid,
                GatewayService.getIdmConfigs().getScheme()+GatewayService.getIdmConfigs().getHostName()+":"+GatewayService.getIdmConfigs().getPort()+GatewayService.getIdmConfigs().getPath(),
                "/"+endpoint,
                HTTPMethod.POST, args, null, null));
        return Response.status(204).header("transaction_id", tid).build();
    }

    @Path("billing/cart/{endpoint}")
    @POST
    @Produces()
    public Response billingCartRequest(@Context HttpHeaders header, @PathParam("endpoint") String endpoint, byte[] args) {
        String tid = TransactionGenerator.generate();
        GatewayService.getThreadPool().putRequest(new ClientRequest(tid,
                GatewayService.getBillingConfigs().getScheme()+GatewayService.getBillingConfigs().getHostName()+":"+GatewayService.getBillingConfigs().getPort()+GatewayService.getBillingConfigs().getPath(),
                "/cart/"+endpoint,
                HTTPMethod.POST, args, null, header));
        return Response.status(204).header("transaction_id", tid).build();
    }

    @Path("billing/order/{endpoint}")
    @POST
    @Produces()
    public Response billingOrderRequest(@Context HttpHeaders header, @PathParam("endpoint") String endpoint, byte[] args) {
        String tid = TransactionGenerator.generate();
        GatewayService.getThreadPool().putRequest(new ClientRequest(tid,
                GatewayService.getBillingConfigs().getScheme()+GatewayService.getBillingConfigs().getHostName()+":"+GatewayService.getBillingConfigs().getPort()+GatewayService.getBillingConfigs().getPath(),
                "/order/"+endpoint,
                HTTPMethod.POST, args, null, header));
        return Response.status(204).header("transaction_id", tid).build();
    }

    @Path("billing/order/complete")
    @GET
    @Produces()
    public Response billingRequest(@QueryParam("token") String token, @QueryParam("PayerID") String payer_id) {
        String tid = TransactionGenerator.generate();
        HashMap<String, Object> map = new HashMap<>();
        map.put("token", token);
        map.put("PayerID", payer_id);
        GatewayService.getThreadPool().putRequest(new ClientRequest(tid,
                GatewayService.getBillingConfigs().getScheme()+GatewayService.getBillingConfigs().getHostName()+":"+GatewayService.getBillingConfigs().getPort()+GatewayService.getBillingConfigs().getPath(),
                GatewayService.getBillingConfigs().getOrderCompletePath(),
                HTTPMethod.GET, null, map, null));
        return Response.status(204).header("transaction_id", tid).build();
    }

    @Path("movies/search")
    @GET
    @Produces()
    public Response moviesSearch(@Context HttpHeaders headers, @QueryParam("title") String title,
                                 @QueryParam("year") int year, @QueryParam("director") String director,
                                 @QueryParam("genre") String genre, @QueryParam("hidden") Boolean hidden,
                                 @QueryParam("limit") int limit, @QueryParam("offset") int offset,
                                 @QueryParam("orderby") String orderby, @QueryParam("direction") String direction) {
        String tid = TransactionGenerator.generate();
        HashMap<String, Object> map = new HashMap<>();
        map.put("title", title);
        map.put("year", year);
        map.put("director", director);
        map.put("genre", genre);
        map.put("hidden", hidden);
        map.put("limit", limit);
        map.put("offset", offset);
        map.put("orderby", orderby);
        map.put("direction", direction);
        GatewayService.getThreadPool().putRequest(new ClientRequest(tid,
                GatewayService.getMoviesConfigs().getScheme()+GatewayService.getMoviesConfigs().getHostName()+":"+GatewayService.getMoviesConfigs().getPort()+GatewayService.getMoviesConfigs().getPath(),
                GatewayService.getMoviesConfigs().getSearchPath(),
                HTTPMethod.GET, null, map, headers));
        return Response.status(204).header("transaction_id", tid).build();
    }

    @Path("movies/browse/{phrase}")
    @GET
    @Produces()
    public Response moviesBrowse(@Context HttpHeaders headers, @PathParam("phrase") String phrase,
                                 @QueryParam("limit") Integer limit, @QueryParam("offset") Integer offset,
                                 @QueryParam("orderby") String orderby, @QueryParam("direction") String direction) {
        String tid = TransactionGenerator.generate();
        HashMap<String, Object> map = new HashMap<>();
        map.put("phrase", phrase);
        map.put("limit", limit);
        map.put("offset", offset);
        map.put("orderby", orderby);
        map.put("direction", direction);
        GatewayService.getThreadPool().putRequest(new ClientRequest(tid,
                GatewayService.getMoviesConfigs().getScheme()+GatewayService.getMoviesConfigs().getHostName()+":"+GatewayService.getMoviesConfigs().getPort()+GatewayService.getMoviesConfigs().getPath(),
                GatewayService.getMoviesConfigs().getBrowsePath(),
                HTTPMethod.GET, null, map, headers));
        return Response.status(204).header("transaction_id", tid).build();
    }

    @Path("movies/get/{movie_id}")
    @GET
    @Produces()
    public Response moviesGet(@Context HttpHeaders headers, @PathParam("movie_id") String movie_id) {
        String tid = TransactionGenerator.generate();
        HashMap<String, Object> map = new HashMap<>();
        map.put("movie_id", movie_id);
        GatewayService.getThreadPool().putRequest(new ClientRequest(tid,
                GatewayService.getMoviesConfigs().getScheme()+GatewayService.getMoviesConfigs().getHostName()+":"+GatewayService.getMoviesConfigs().getPort()+GatewayService.getMoviesConfigs().getPath(),
                GatewayService.getMoviesConfigs().getGetPath(),
                HTTPMethod.GET, null, map, headers));
        return Response.status(204).header("transaction_id", tid).build();
    }

    @Path("movies/thumbnail")
    @POST
    @Produces()
    public Response moviesThumb(@Context HttpHeaders headers, byte[] args) {
        String tid = TransactionGenerator.generate();
        GatewayService.getThreadPool().putRequest(new ClientRequest(tid,
                GatewayService.getMoviesConfigs().getScheme()+GatewayService.getMoviesConfigs().getHostName()+":"+GatewayService.getMoviesConfigs().getPort()+GatewayService.getMoviesConfigs().getPath(),
                GatewayService.getMoviesConfigs().getThumbnailPath(),
                HTTPMethod.POST, args, null, headers));
        return Response.status(204).header("transaction_id", tid).build();
    }

    @Path("movies/people")
    @GET
    @Produces()
    public Response moviesPeople(@Context HttpHeaders headers, @QueryParam("name") String name,
                                 @QueryParam("limit") int limit, @QueryParam("offset") int offset,
                                 @QueryParam("orderby") String orderby, @QueryParam("direction") String direction) {
        String tid = TransactionGenerator.generate();
        HashMap<String, Object> map = new HashMap<>();
        map.put("name", name);
        map.put("limit", limit);
        map.put("offset", offset);
        map.put("orderby", orderby);
        map.put("direction", direction);
        GatewayService.getThreadPool().putRequest(new ClientRequest(tid,
                GatewayService.getMoviesConfigs().getScheme()+GatewayService.getMoviesConfigs().getHostName()+":"+GatewayService.getMoviesConfigs().getPort()+GatewayService.getMoviesConfigs().getPath(),
                GatewayService.getMoviesConfigs().getPeoplePath(),
                HTTPMethod.GET, null, map, headers));
        return Response.status(204).header("transaction_id", tid).build();
    }

    @Path("movies/people/search")
    @GET
    @Produces()
    public Response moviesPeopleSearch(@Context HttpHeaders headers, @QueryParam("name") String name,
                                       @QueryParam("birthday") String birthday, @QueryParam("movie_title") String movie_title,
                                       @QueryParam("limit") int limit, @QueryParam("offset") int offset,
                                       @QueryParam("orderby") String orderby, @QueryParam("direction") String direction) {
        String tid = TransactionGenerator.generate();
        HashMap<String, Object> map = new HashMap<>();
        map.put("name", name);
        map.put("birthday", birthday);
        map.put("movie_title", movie_title);
        map.put("limit", limit);
        map.put("offset", offset);
        map.put("orderby", orderby);
        map.put("direction", direction);
        GatewayService.getThreadPool().putRequest(new ClientRequest(tid,
                GatewayService.getMoviesConfigs().getScheme()+GatewayService.getMoviesConfigs().getHostName()+":"+GatewayService.getMoviesConfigs().getPort()+GatewayService.getMoviesConfigs().getPath(),
                GatewayService.getMoviesConfigs().getPeopleSearchPath(),
                HTTPMethod.GET, null, map, headers));
        return Response.status(204).header("transaction_id", tid).build();
    }

    @Path("movies/people/get/{person_id}")
    @GET
    @Produces()
    public Response moviesPeopleGet(@Context HttpHeaders headers, @PathParam("person_id") Integer person_id) {
        String tid = TransactionGenerator.generate();
        HashMap<String, Object> map = new HashMap<>();
        map.put("person_id", person_id);
        GatewayService.getThreadPool().putRequest(new ClientRequest(tid,
                GatewayService.getMoviesConfigs().getScheme()+GatewayService.getMoviesConfigs().getHostName()+":"+GatewayService.getMoviesConfigs().getPort()+GatewayService.getMoviesConfigs().getPath(),
                GatewayService.getMoviesConfigs().getPeopleGetPath(),
                HTTPMethod.GET, null, map, headers));
        return Response.status(204).header("transaction_id", tid).build();
    }

    @Path("report")
    @GET
    @Produces()
    public Response report(@Context HttpHeaders header) {
        Connection con = GatewayService.getConnectionPoolManager().requestCon();
        Connection con1 = GatewayService.getConnectionPoolManager().requestCon();
        try {
            String query = "SELECT * FROM responses WHERE transaction_id = ?";
            PreparedStatement ps = con.prepareStatement(query);
            ps.setString(1, header.getHeaderString("transaction_id"));
            ServiceLogger.LOGGER.info("Extracting records, Executing Query: "+ps.toString());
            ResultSet rs = ps.executeQuery();
            String response;
            int status;
            if (rs.next()) {
                response = rs.getString(4);
                status = rs.getInt(5);
            } else {
                return Response.status(204).entity(null).build();
            }
            ServiceLogger.LOGGER.info("Deleting records, Executing Query: "+ps.toString());
            String query1 = "DELETE FROM responses WHERE transaction_id = ?";
            PreparedStatement ps1 = con.prepareStatement(query1);
            ps1.setString(1, header.getHeaderString("transaction_id"));
            ps1.executeUpdate();
            return Response.status(status).entity(response).build();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            GatewayService.getConnectionPoolManager().releaseCon(con);
            GatewayService.getConnectionPoolManager().releaseCon(con1);
        }
        return Response.status(204).entity(null).build();
    }


}
