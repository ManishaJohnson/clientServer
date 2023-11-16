import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.*;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class KeyValueStore {
	private static ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
    private static final Object putLock = new Object(); // Lock for synchronization
    private static final int TCP_PORT = 4999;
    private static final int UDP_PORT = 5000;
    private static ExecutorService tcpThreadPool = Executors.newCachedThreadPool();
    private static ExecutorService threadPool = Executors.newCachedThreadPool();
    private static volatile boolean shutdownServer = false;
    private static final Object socketLock = new Object();
    
    //main code to handle redirection for server-client connection using udp, tcp, rmi
    public static void main(String[] args) throws IOException {	 
    	if (args.length < 2) {
            System.out.println("Usage: java KeyValueStore [tcp/udp] [server/client]");
            return;
        }

        String protocol = args[0].toLowerCase();
        String mode = args[1].toLowerCase();

        switch (protocol) {
        case "tcp":
            if ("server".equals(mode)) {
                startTcpServer();
            } else if ("client".equals(mode)) {
                startTcpClient();
            }
            break;
        case "udp":
            if ("server".equals(mode)) {
                startUdpServer();
            } else if ("client".equals(mode)) {
                startUdpClient();
            }
            break;
        case "rmi":
            if ("server".equals(mode)) {
                startRmiServer();
            } else if ("client".equals(mode)) {
                startRmiClient();
            }
            break;
        default:
            System.out.println("Invalid protocol. Use 'tcp', 'udp', or 'rmi'.");
        }
    }
    
    // TCP Server code
    private static ServerSocket serverSocket = null;
    private static void startTcpServer() {
    	
        try {
            serverSocket = new ServerSocket(TCP_PORT);
            System.out.println("TCP Server started on port " + TCP_PORT);

            while (!shutdownServer) {
                try {
                    System.out.println("TCP Server waiting for client connections...");
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("TCP Client connected: " + clientSocket.getInetAddress());

                    tcpThreadPool.submit(() -> handleClient(clientSocket));
                } catch (SocketException e) {
                    if (shutdownServer) {
                        System.out.println("Server is shutting down.");
                        break;
                    }
                    System.err.println("SocketException in server accept: " + e.getMessage());
                }
            }
            
        } catch (IOException e) {
            System.err.println("Exception in TCP Server: " + e.getMessage());
        } finally {

            //closeServerSocket();
        	try {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    serverSocket.close();
                    System.out.println("TCP Server socket closed.");
                }
            } catch (IOException e) {
                System.err.println("Error closing server socket: " + e.getMessage());
            }
        	
            //shutdownTcpThreadPool();
        	tcpThreadPool.shutdown();
            try {
                if (!tcpThreadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                    tcpThreadPool.shutdownNow();
                    System.out.println("TCP thread pool shutdown now.");
                }
            } catch (InterruptedException ie) {
                tcpThreadPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                String response = processCommand(inputLine);
                out.println(response);

                if ("exit".equalsIgnoreCase(inputLine.trim())) {
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("Exception with client communication: " + e.getMessage());
        } finally {
        	try {
                if (clientSocket != null && !clientSocket.isClosed()) {
                    clientSocket.close();
                    System.out.println("Client socket closed.");
                }
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }


    // TCP Client code
    private static void startTcpClient() {
    	int port = TCP_PORT;
        Socket s;
		try {
			s = new Socket("localhost", port);
			
			//Logic
			clientProcess(s);
		}catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Could not connect to the TCP server. Please ensure the TCP server is running.");
			e.printStackTrace();
		}
    }

    public static void clientProcess(Socket sktserver) {
    	PrintWriter pr;
    	BufferedReader in;
    	long startTime = System.nanoTime();;
    	String[] command = {};
		try {
			pr = new PrintWriter(sktserver.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(sktserver.getInputStream()));
			
			//logic
	        
	        do {
	        	command = getClientCommand();
	        	pr.println(String.join(" ", command));

	            if ("exit".equals(command[0])) {
	                //System.out.println("Exit command sent. Closing client socket."); // Debug line
	                break; // Exit the loop before waiting for server's response
	            }
	            // Read server response if expected
	            String response = in.readLine();
	            System.out.println("Server says: " + response);
	            
	        }while (!"exit".equals(command[0]));
	        System.out.println("Exiting client process."); // Debug line
		} catch (IOException e) {
			System.out.println("Exception in client process: " + e.getMessage()); // Debug line
	        e.printStackTrace();
		}finally {
			
			long endTime = System.nanoTime();
            long duration = (endTime - startTime)/1_000_000; // milliseconds
            System.out.println("Operation '" + command[0] + "' executed in " + duration + " ms");
            
	        try {
	            if (sktserver != null && !sktserver.isClosed()) {
	                sktserver.close(); // Ensure the socket is closed
	                System.out.println("Client socket closed."); // Debug line
	            }
	        } catch (IOException e) {
	            System.out.println("Exception while closing client socket: " + e.getMessage()); // Debug line
	            e.printStackTrace();
	        }
	    }
    }
    private static DatagramSocket udpSocket = null;
    private static void startUdpServer() {

    	try  {
    		udpSocket = new DatagramSocket(UDP_PORT);
            byte[] buffer = new byte[65535];

            while (!shutdownServer) {
            	System.out.println("UDP Server waiting for packets"); // Debug line
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                
                try {
                    udpSocket.receive(packet);
                } catch (SocketException e) {
                    if (shutdownServer) {
                        System.out.println("UDP Server shutdown initiated, stopping packet reception.");
                        break;
                    }
                    throw e;
                }

                Runnable task = new ClientHandler(udpSocket, packet);
                threadPool.submit(task);
            }
            System.out.println("UDP Server is shutting down."); 
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        	
        	//close udpSocket
        	if (udpSocket != null && !udpSocket.isClosed()) {
                udpSocket.close();
                System.out.println("UDP Socket closed.");
            }
        	
        	//Shutdown ThreadPool
            threadPool.shutdown(); // Graceful shutdown of the thread pool
            try {
                if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                    threadPool.shutdownNow();
                    System.out.println("Forced shutdown of thread pool executed.");
                }
            } catch (InterruptedException ie) {
                threadPool.shutdownNow();
            }
        }
    }
    

    private static class ClientHandler implements Runnable {
        private final DatagramSocket socket;
        private final DatagramPacket packet;

        ClientHandler(DatagramSocket socket, DatagramPacket packet) {
            this.socket = socket;
            this.packet = new DatagramPacket(packet.getData(), packet.getLength(), packet.getAddress(), packet.getPort());
        }

        @Override
        public void run() {
            if (shutdownServer) {
                return;
            }
            try {
                String received = new String(packet.getData(), 0, packet.getLength());
                //System.out.println("ClientHandler received: " + received); // Debug line
                String response = processCommand(received);

                synchronized (socketLock) {
                    if (!shutdownServer) {
                        byte[] sendData = response.getBytes();
                        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, packet.getAddress(), packet.getPort());
                        socket.send(sendPacket);
                    }
                }
                
                if (shutdownServer) {
                    udpSocket.close(); // Close the socket to interrupt the blocking receive call
                }
            }  catch (IOException e) {
                if (!shutdownServer || !(e instanceof SocketException)) {
                    e.printStackTrace();
                }
            }
        }
    }


    private static void startUdpClient() {
    	DatagramSocket socket = null;
        BufferedReader stdIn = null;
        
        long startTime = System.nanoTime(); //perf
        
    	try {
            socket = new DatagramSocket();
            InetAddress serverAddress = InetAddress.getByName("localhost");
            
            while (true) {
            	String[] commandParts = getClientCommand();

            	String command = String.join(" ", commandParts);
                //System.out.println("Client sending command: " + command); // Debug line
                
                
                
                if (!command.trim().isEmpty()) {
                    //System.out.println("Formatted command: " + command); // Debug line
                    sendCommand(socket, serverAddress, UDP_PORT, command);
                    
                } else {
                    System.out.println("Invalid command format"); // Debug line
                }

                //sendCommand(socket, serverAddress, UDP_PORT, command);
                if ("exit".equals(command)) {
                	System.out.println("startUdpClient-break stmt on exit");// Debug line
                	break;
                }

                if (!"invalid".equals(commandParts[0]) && !"error".equals(commandParts[0])) {
                    String response = receiveResponse(socket);
                    System.out.println("Server says: " + response);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        	long endTime = System.nanoTime();
            long duration = (endTime - startTime)/1_000_000; // Convert to milliseconds
            System.out.println("UDP Operation executed in " + duration + " ms");
        	
            if (socket != null) {
                socket.close();
            }
            try {
                if (stdIn != null) {
                    stdIn.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    private static void sendCommand(DatagramSocket socket, InetAddress serverAddress, int serverPort, String command) throws IOException {
    	try {
    		//System.out.println("Sending command: " + command); // Debug line
	        byte[] sendData = command.getBytes();
	        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, serverAddress, serverPort);
	        socket.send(sendPacket);
    	} catch (SocketTimeoutException e) {
            //SocketTimeoutExecption
        }
    }

    private static String receiveResponse(DatagramSocket socket) throws IOException {
    	byte[] receiveData = new byte[65535];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        socket.receive(receivePacket);
        return new String(receivePacket.getData(), 0, receivePacket.getLength());
    }
      
    private static String processCommand(String command) {
    	//System.out.println("Processing command: " + command); // Debug line
        String[] parts = command.split(" ");
        //System.out.println("Command parts: " + Arrays.toString(parts)); // Debug line
        switch (parts[0].toLowerCase()) {
            case "put":
                return handlePut(parts);
            case "get":
                return handleGet(parts);
            case "del":
                return handleDel(parts);
            case "store": // Handle 'store' command
                return Store();
            case "test":
                return handleTest(); // Handle 'test' command
            case "getlen":
                return handleGetLen(); // Handle 'getlen' command
            case "exit":
            	//System.out.println("Exit command received, shutting down server."); // Debug line
            	shutdownServer = true;
            	try {
                    if (serverSocket != null && !serverSocket.isClosed()) {
                        serverSocket.close();
                    }
                } catch (IOException e) {
                    System.err.println("Error closing server socket: " + e.getMessage());
                    e.printStackTrace();
                }
                return "Server shutting down.";
            default:
                return "ERROR: Unknown command.";
        }
    }
    

    private static String[] getClientCommand() {
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
        String[] command = new String[3]; // Command, Key, Value
        try {
            System.out.println("\nChoose an action:");
            System.out.println("1. Put");
            System.out.println("2. Get");
            System.out.println("3. Del");
            System.out.println("4. Store");
            System.out.println("5. Exit");
            System.out.println("6. Test");
            System.out.println("7. Length");
            System.out.println("8. Test2");
            System.out.print("Enter choice: ");

            String userInput = stdIn.readLine();
            switch (userInput) {
                case "1": // Put
                    System.out.print("Enter key: ");
                    command[1] = stdIn.readLine();
                    System.out.print("Enter value: ");
                    command[2] = stdIn.readLine();
                    command[0] = "put";
                    break;
                case "2": // Get
                	command = new String[2];
                    System.out.print("Enter key: ");
                    command[1] = stdIn.readLine();
                    command[0] = "get";
                    break;
                case "3": // Del
                	command = new String[2];
                    System.out.print("Enter key: ");
                    command[1] = stdIn.readLine();
                    command[0] = "del";
                    break;
                case "4": // Store
                    command[0] = "store";
                    break;
                case "5": // Exit
                	command = new String[1];
                    command[0] = "exit";
                    //System.out.println("Client Shutting down"); // Debug line
                    break;
                case "6": // Test
                    command[0] = "test";
                    break;
                case "7": // Length
                    command[0] = "getlen";
                    break;
                case "8": // Test2
                    command[0] = "test2";
                    break;
                default:
                    System.out.println("Invalid choice. Please enter a number between 1 and 8.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return command;
    }

    private static String handlePut(String[] parts) {
        if (parts.length != 3) {
            return "ERROR: Usage: put <key> <value>";
        }
        synchronized (putLock) { // Synchronize for concurrent write access
            map.put(parts[1], parts[2]);
            return "OK";
        }
    }

    private static String handleGet(String[] parts) {
        if (parts.length != 2) {
        	 return "ERROR: Usage: get <key> - Received parts: " + Arrays.toString(parts);
        }
        String value = map.get(parts[1]);
        return value != null ? value : "NOT FOUND";
    }

    private static String handleDel(String[] parts) {
        if (parts.length != 2) {
        	return "ERROR: Usage: del <key> - Received parts: " + Arrays.toString(parts);
        }
        return map.remove(parts[1]) != null ? "OK" : "NOT FOUND";
    }
    
    private static String Store() {
        StringBuilder sb = new StringBuilder();
        map.forEach((key, value) -> sb.append(key).append("=").append(value).append("; "));
        String fullContents = sb.toString();
        if (fullContents.length() > 65000) {
            // Trim the contents if too long
            String trimmedContents = "TRIMMED:" + fullContents.substring(0, 65000) + ".....trimmed";
            return trimmedContents;
        } else {
            return fullContents;
        }
    }
    
    private static String handleTest() {
        int numberOfEntriesNeeded = 700; // Adjust if necessary based on the size of keys and values
        for (int i = 0; i < numberOfEntriesNeeded; i++) {
            String key = "key" + i;
            // Create a value that is 90 characters long
            String value = "value" + i + new String(new char[80]).replace('\0', 'x');
            map.put(key, value);
        }

        // Step 2: Trigger the `store` command
        String storeContents = Store();
        return "TEST COMPLETED: Store contents length: " + storeContents.length();
    }

    private static String handleGetLen() {
        String contentLength = String.valueOf(Store().length());
        return "LENGTH:" + contentLength;
    }
    
    //RMI
    private static void startRmiServer() {
    	try {
            KeyValueStoreServer server = new KeyValueStoreServerImpl();
            Registry registry = LocateRegistry.createRegistry(1099);
            registry.rebind("KeyValueStoreServer", server);
            System.out.println("Server is ready.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    

    private static void startRmiClient() {
    	
    	//perf
    	long startTime = System.nanoTime();
        // Client logic here
        try {
            Registry registry = LocateRegistry.getRegistry("localhost", 1099);
            KeyValueStoreServer server = (KeyValueStoreServer) registry.lookup("KeyValueStoreServer");
            
            Scanner scanner = new Scanner(System.in);
            String userInput;
            do {
                System.out.println("\nChoose an action:");
                System.out.println("1. Put");
                System.out.println("2. Get");
                System.out.println("3. Del");
                System.out.println("4. Store");
                System.out.println("5. Exit");
                System.out.println("6. Test");
                System.out.println("7. Length");
                System.out.println("8. Test2");
                System.out.print("Enter choice: ");

                userInput = scanner.nextLine();
                
                switch (userInput) {
                    case "1": // Put
                        System.out.print("Enter key: ");
                        String key = scanner.nextLine();
                        System.out.print("Enter value: ");
                        String value = scanner.nextLine();
                        System.out.println(server.put(key, value));
                        break;
                    case "2": // Get
                        System.out.print("Enter key: ");
                        key = scanner.nextLine();
                        System.out.println(server.get(key));
                        break;
                    case "3": // Del
                        System.out.print("Enter key: ");
                        key = scanner.nextLine();
                        System.out.println(server.del(key));
                        break;
                    case "4": // Store
                        System.out.println(server.store());
                        break;
                    case "5": // Exit
                        try {
                            System.out.println("Exiting...");
                            server.shutdown(); // Call the shutdown method on the server
                        } catch (UnmarshalException e) {
                            System.err.println("Client closed");
                        } finally {
                            
                        }
                        break;
                    case "6":
                        System.out.println(server.test());
                        break;
                    case "7":
                        System.out.println(server.getLen());
                        break;
                    case "8":
                        System.out.println(server.test2());
                        break;
                    default:
                        System.out.println("Invalid choice. Please enter 1, 2, 3, 4, or 5.");
                        break;
                }
            } while (!userInput.equals("5"));

            scanner.close();

            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1_000_000; // Convert to milliseconds
            System.out.println("RMI Operation executed in " + duration + " ms");

        } catch (RemoteException e) {
            System.err.println("Could not connect to the server. Please ensure the RMI server is running.");
        }  catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }

    public interface KeyValueStoreServer extends Remote {
    	String put(String key, String value) throws RemoteException;
        String get(String key) throws RemoteException;
        String del(String key) throws RemoteException;
        String store() throws RemoteException;
        String test() throws RemoteException;
        String getLen() throws RemoteException;
        String test2() throws RemoteException;
        void shutdown() throws RemoteException;
    }

    public static class KeyValueStoreServerImpl extends UnicastRemoteObject implements KeyValueStoreServer {
    	private static final long serialVersionUID = 1L;
        private static ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

        protected KeyValueStoreServerImpl() throws RemoteException {
            super();
        }

        @Override
        public String put(String key, String value) throws RemoteException {
            synchronized (map) {
                map.put(key, value);
                System.out.println("Entry added. Current store size: " + map.size());
                return "OK";
            }
        }

        @Override
        public String get(String key) throws RemoteException {
            String value = map.get(key);
            return value != null ? value : "NOT FOUND";
        }

        @Override
        public String del(String key) throws RemoteException {
            String previousValue = map.remove(key);
            return (previousValue != null) ? (key + " deleted successfully") : ("NOT FOUND: " + key);
        }

        @Override
        public String store() throws RemoteException {
            StringBuilder sb = new StringBuilder();
            map.forEach((k, v) -> sb.append(k).append("=").append(v).append("; "));
            String fullContents = sb.toString();
            return (fullContents.length() > 65000) ?
                    ("TRIMMED:" + fullContents.substring(0, 65000) + ".....trimmed") : fullContents;
        }

        @Override
        public String test() throws RemoteException {
            int numberOfEntriesNeeded = 700;
            for (int i = 0; i < numberOfEntriesNeeded; i++) {
                String key = "key" + i;
                String value = "value" + i + new String(new char[80]).replace('\0', 'x');
                map.put(key, value);
            }
            String storeContents = store();
            return "TEST COMPLETED: Store contents length: " + storeContents.length();
        }

        @Override
        public String getLen() throws RemoteException {
            return "LENGTH:" + store().length();
        }

        @Override
        public String test2() throws RemoteException {
            int mapSize = map.size();
            StringBuilder firstKeys = new StringBuilder("First keys:");
            int count = 0;
            for (String key : map.keySet()) {
                if (count >= 10) break;
                firstKeys.append(" ").append(key);
                count++;
            }
            return "TEST2 COMPLETED: Size of store: " + mapSize + "; " + firstKeys.toString();
        }
        public void shutdown() throws RemoteException {
            System.out.println("Server is shutting down...");
            System.exit(0); // Terminate the server process
        }
    }
}
