package coordinator;

import java.io.*;
import java.net.*;

public class Coordinator {

    public static void main(String[] args) throws Exception {
        String workerIp = "13.53.197.35"; // <-- сюда IP EC2
        int port = 9000;

        Socket socket = new Socket(workerIp, port);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

        out.println("HELLO FROM COORDINATOR");

        socket.close();
        System.out.println("Message sent to worker");
    }
}
