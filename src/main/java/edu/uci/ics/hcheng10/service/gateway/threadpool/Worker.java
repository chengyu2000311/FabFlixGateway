package edu.uci.ics.hcheng10.service.gateway.threadpool;

import edu.uci.ics.hcheng10.service.gateway.GatewayService;
import edu.uci.ics.hcheng10.service.gateway.logger.ServiceLogger;

import javax.xml.ws.Response;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Worker extends Thread {
    int id;
    ThreadPool threadPool;

    private Worker(int id, ThreadPool threadPool) {
        this.id = id;
        this.threadPool = threadPool;
    }

    public static Worker CreateWorker(int id, ThreadPool threadPool) {
        return new Worker(id, threadPool);
    }

    public void process(ClientRequest request) {
        request.sendRequest();
    }

    @Override
    public void run() {
        while (true) {
            ServiceLogger.LOGGER.info(String.format("Worker%d started", this.id));
            ClientRequest request = this.threadPool.takeRequest();
            ServiceLogger.LOGGER.info("Starting processing of request "+ request.getTransaction_id());
            try{this.process(request);} catch (Exception e) {e.printStackTrace();}
        }
    }
}
