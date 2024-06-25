package benchmark;

import in.prasannathapa.db.HashDB;
import in.prasannathapa.db.data.Data;
import in.prasannathapa.db.remote.Server;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class BenchmarkApp {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage:");
            System.out.println("  java -jar app.jar -test -h <host> -p <port> -e <entries> -b <batch size>");
            System.out.println("  java -jar app.jar -test -e <entries>");
            System.out.println("  java -jar app.jar -serve -p <port>");
            return;
        }

        if (args[0].equals("-test")) {
            handleTestCommand(args);
        } else if (args[0].equals("-serve")) {
            handleServeCommand(args);
        } else {
            System.out.println("Invalid command: " + args[0]);
        }
    }

    private static void handleTestCommand(String[] args) {
        try {
            String host = null; // Default host
            int port = 1099; // Default port
            int entries = 500_000; // Default entries
            int batchSize = 200; // Default batch size

            // Parse arguments
            int i = 1;
            while (i < args.length - 1) {
                switch (args[i]) {
                    case "-h":
                        host = args[i + 1];
                        i += 2;
                        break;
                    case "-p":
                        port = Integer.parseInt(args[i + 1]);
                        i += 2;
                        break;
                    case "-e":
                        entries = Integer.parseInt(args[i + 1]);
                        i += 2;
                        break;
                    case "-b":
                        batchSize = Integer.parseInt(args[i + 1]);
                        i += 2;
                        break;
                    default:
                        System.out.println("Invalid argument: " + args[i]);
                        return;
                }
            }

            if (host != null && port > 0 && entries > 0 && batchSize > 0) {
                // Remote benchmarking
                Options opt = new OptionsBuilder()
                        .include(RemoteBenchmark.class.getSimpleName())
                        .forks(1)
                        .param("host", host)
                        .param("port", Integer.toString(port))
                        .param("entries", Integer.toString(entries))
                        .param("batchSize", Integer.toString(batchSize))
                        .build();
                new Runner(opt).run();
            } else if(host == null && entries > 0) {
                Options opt = new OptionsBuilder()
                        .include(LocalBenchmark.class.getSimpleName())
                        .param("entries", Integer.toString(entries))
                        .forks(1)
                        .build();
                new Runner(opt).run();
            } else {
                System.out.println("Invalid Arguments");
            }
        } catch (NumberFormatException | RunnerException e) {
            System.out.println("Error parsing arguments: " + e.getMessage());
        }
    }

    private static void handleServeCommand(String[] args) {
        int port = 1099; //default port
        if(args.length == 2){
            port = Integer.parseInt(args[1]);
        } if(args.length > 2) {
            System.out.println("Invalid Arguments");
            return;
        }
        try {
            Server obj = new Server();
            Registry registry = LocateRegistry.createRegistry(port);
            registry.bind(HashDB.class.getName(), obj);
            System.out.println("HashDB Server Started");
            System.out.println("bind: " + HashDB.class.getName() + " port: " + port);
        } catch (Exception e) {
            System.err.println("Server exception: " + e);
        }
    }
}
