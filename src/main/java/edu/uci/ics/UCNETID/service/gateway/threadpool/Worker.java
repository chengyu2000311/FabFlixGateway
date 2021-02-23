package edu.uci.ics.UCNETID.service.gateway.threadpool;

public class Worker extends Thread {
    int id;
    ThreadPool threadPool;

    private Worker(int id, ThreadPool threadPool) {

    }

    public static Worker CreateWorker(int id, ThreadPool threadPool) {
        return null;
    }

    public void process() {

    }

    @Override
    public void run() {
        while (true) {

        }
    }
}
