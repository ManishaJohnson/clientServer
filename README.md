# KeyValueStore Program

Group details:
2022H1120281P - Manisha K Johnson
2022H1120286P - Jyotsana Chandrakar

KeyValueStore is a key-value store application in Java, supporting multiple protocols including TCP, UDP, and RMI for server-client communication. It allows users to store, retrieve, and delete key-value pairs through a network. The server can handle multiple client requests concurrently.

Flow of Execution
Start the Program: When the program starts, the user should be given a choice to select between TCP, UDP, or RMI mode.
TCP Mode:
•   If TCP is selected, the program will function as it currently does with the TCP client-server model.
UDP Mode:
•   If UDP is selected, the program will start UDP client and server threads. The client will communicate with the server using UDP datagrams.
RMI Mode:
•   If RMI is selected, the program will start the RMI registry, register the remote object, and then the client can interact with the server using remote method calls.

## Prerequisites
- Java Development Kit (JDK) 8 or higher.
- Network access for client-server communication.

## Setup

1. Clone the repository to your local machine.
   ```
   git clone https://github.com/ManishaJohnson/clientServer.git 
   ```

2. Navigate to the project directory.
   ```
   cd [../2022H1120281P]
   ```

## Running the Simulation

1. Navigate to the directory where the files are located and compile the Java files:
   ```
   javac KeyValueStore.java
   ```
2. To start the server, use one of the following commands depending on the desired protocol:.
   ```
   java KeyValueStore tcp server
   java KeyValueStore udp server
   java KeyValueStore rmi server
   ```

2. To start the client, use one of the following commands based on the protocol:
   ```
   java KeyValueStore tcp client
   java KeyValueStore udp client
   java KeyValueStore rmi client
   ```


