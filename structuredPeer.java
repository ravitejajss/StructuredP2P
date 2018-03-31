import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class structuredPeer {
	public static boolean isSearchComplete;

	private static Logger logger = Logger.getLogger("NodeLog");
	private static String BS_ip;
	private static int BS_port;
	private static String myIP;
	private static int myPort;
	private static FileHandler log_file;
	private static Socket sock;
	private static List<String> nodeResources = Collections.synchronizedList(new ArrayList<String>());
	private static FingerTable fingerTable;
	private static ConcurrentHashMap<Integer, ArrayList<Integer>> keyTable = new ConcurrentHashMap<Integer,ArrayList<Integer>>();
	                              // fileKey  nodeKey
	private static ConcurrentHashMap<Integer, String> allFileDetails = new ConcurrentHashMap<Integer,String>();
	                              // fileKey  fileName
	private static int myKey;
	private static int totalNodeCount = 20;
	private static final int m = 15;
	private static int succKey;
	private static String succIp;
	private static int succPort;
	private static Scanner sc = new Scanner(System.in);						// Catching the input from the Keyboard.
	
	public static String uname = "struct";
	
	public static void main(String[] args) throws NoSuchAlgorithmException {
		
		try {
			
			if (Arrays.asList(args).contains("--help") || Arrays.asList(args).contains("-h")) {
				StringBuilder helpBuilder = new StringBuilder();
		        helpBuilder.append("java structuredPeer <portnum> <bootstrap ip> <bootstrap port>");
		        helpBuilder.append(System.getProperty("line.separator"));
		        helpBuilder.append("All arguments are required.");
		        helpBuilder.append(System.getProperty("line.separator"));
		        helpBuilder.append("<portnum> is the port number at which the machine has to be listening");
		        helpBuilder.append(System.getProperty("line.separator"));
		        helpBuilder.append("<bootstrap ip> is the ip address of the bootstrapper");
		        helpBuilder.append(System.getProperty("line.separator"));
		        helpBuilder.append("<bootstrap port> is the port num at which the bootstrapper will be listening");
		        helpBuilder.append(System.getProperty("line.separator"));
		        System.out.println(helpBuilder.toString());
	            exit(0);            
	        }
			
			log_file = new FileHandler("Node.Log");                       //Providing a file to the file handler.
			SimpleFormatter formatter = new SimpleFormatter();            //Logging in Human readable format.
			log_file.setFormatter(formatter);
			logger.addHandler(log_file);
			logger.setUseParentHandlers(false);
			
			BS_ip = args[1];
			BS_port = Integer.parseInt(args[2]);
			myPort = Integer.parseInt(args[0]);
			myIP = InetAddress.getLocalHost().getHostAddress();
			String sockAdd = myIP+":"+Integer.toString(myPort);
			
			if ((myPort <= 5000 || myPort >= 65535) || (BS_port <= 5000 || BS_port >= 65535 )) {		// Handling Port Exceptions.
				System.out.println("ERROR: Please type an integer in the range of 5001 - 65535 for port number(s).");
				logger.log(Level.WARNING, "User assigned a port number which is out of port ranges.");
				exit(1);
			}
			
			myKey = hash(sockAdd);
			fingerTable = new FingerTable(myKey, myIP, myPort, m, totalNodeCount);
			System.out.println("Fingertable initiated"); //test print
			
			peerListen lis = new peerListen(myPort, myIP, keyTable, allFileDetails, fingerTable, uname);
			lis.start();
			System.out.println("listen thread will start here"); //test print
			
			int noOfResources = 5;
			LoadResources(noOfResources);
			System.out.println("loaded resources"); //test print
			resgisterNode();
			
			AddResourcesToNetwork();
			System.out.println("Added resources to n/w"); //test print
			GetKeysFromSuccessor();
			System.out.println("Got keys from succ."); //test print
			System.out.println("you can give commands now"); //test print
			
			while(true) { 
				String s = sc.nextLine();                                        // Accepting the User Entry Commands.
				String[] S = s.split(" ");
				switch (S[0]) {
				case "details":
					System.out.println("\nMy details:\nIP: "+myIP+" Port: "+myPort+" Key: "+myKey+"\n");
					break;
					
				case "fingertable":
					fingerTable.PrintStats();
					break;
					
				case "keytable":
					System.out.println("\nFile     Node");
					for (int i : keyTable.keySet()) {
						ArrayList<String> nodes = new ArrayList<String>();
						for(int j: keyTable.get(i))
							nodes.add(String.format("%05d", j));
						System.out.println(String.format("%05d", i) +"   "+ nodes);
					}
					System.out.println("\n"+nodeResources);
					break;
					
				case "entries":
					System.out.println("\nMy resources are:\n");
					for(String i : nodeResources) {
						System.out.println(i+"\n");
					}
					break;
				
				case "search"://INCOMPLETE: has to deal with part file names
					int key = hash(S[1]);
					search(key);
					break;
					
				case "exit":
					deleteNode();
					break;
					
				case "answered":
					System.out.println("\nAnswered requests: "+ lis.answered);
					break;
					
				case "forwarded":
					System.out.println("\nForwarded requests: "+ lis.forwarded);
					break;	
					
				case "received":
					System.out.println("\nReceived requests: "+ (lis.answered+lis.forwarded));
					break;	
					
				case "DEL":
					String msg = "DEL UNAME "+ uname;
					msg = String.format("%04d", msg.length()) +" "+ msg;
					sendrec(msg, BS_ip, BS_port);
					System.out.println("Network Deleted Successfully from bootstrap. Exiting");
					exit(0);
					break;
					
				default:
					System.out.println("Usage: \n"
							+ "DEL UNAME <username>:                    Deletes the network <username> from Bootstrap Server.\n"
							+ "fingertable:                             Prints finger table.\n"
							+ "entries:                                 Prints resources in this node.\n"
							+ "keytable:                                Prints key table.\n"
							+ "answered:                                Gives the queries ansered till now.\n"
							+ "forwarded:                               Gives the number of queries forwarded till now.\n"
							+ "received:                                Gives the number of queries received till now.\n"
							+ "search <(part of)file name>:             Searches the given file name or part of the file name\n"
							+ "queries <no of qeries> <zipfs exponent>: Generates the number of queries given wit hthat Zipf's exponent.\n"
							+ "exit:                                    Exits the program.\n"
							//add the added features here
							);
					break;
				}
			}
		}
		catch (NumberFormatException nfe) {
			System.err.println("ERROR: Please give integer for port number(s). Exiting");
			logger.log(Level.WARNING, "User gave a port number which has non-integers.");
			nfe.printStackTrace();
			exit(1);
		} catch (IOException ioe) {
			System.err.println("ERROR: IO exception in main. Exiting");
			logger.log(Level.WARNING, "ERROR: IO exception in main");
			ioe.printStackTrace();
			exit(1);
		}
	}
	
	private static void search(int key) throws IOException {
		isSearchComplete = false;
		if(!keyTable.containsKey(key)) {
			String sendMsg = "SER "+ myIP +" "+ myPort +" "+ key;
			int respKey = fingerTable.FindNextKey(key);
			String ip = fingerTable.GetIp(respKey);
			int port = fingerTable.GetPort(respKey);
			Send(sendMsg, ip, port);
		}
		else
		{
			System.out.println("Search successful. Queried file is already in the node");
		}
	}

	private static void GetKeysFromSuccessor() throws IOException, NoSuchAlgorithmException {
		succKey = fingerTable.GetSuccessor(myKey);
		succIp = fingerTable.GetIp(succKey);
		succPort = fingerTable.GetPort(succKey);
		String msg = "GETKY "+ myKey;
		msg = String.format("%04d", msg.length()) +" "+ msg;
		String crudeReply = sendrec(msg, succIp, succPort);
		String[] reply = crudeReply.split(" ");
		for (int i = 3; i < reply.length; i+=4) {
			String ip = reply[i];
			String stringPort = reply[i+1];
			int port = Integer.parseInt(stringPort);
			int fileKey = Integer.parseInt(reply[i+2]);
			int nodeKey = hash(ip+":"+stringPort);
			fingerTable.AddDetails(nodeKey, ip, port);
			if(keyTable.containsKey(fileKey)) {
				ArrayList<Integer> nodesHasFile = keyTable.get(fileKey);
				nodesHasFile.add(nodeKey);
				keyTable.put(fileKey, nodesHasFile);
			}
			else {
				ArrayList<Integer> nodesHasFile = new ArrayList<Integer>();
				nodesHasFile.add(nodeKey);
				keyTable.put(fileKey, nodesHasFile);
			}
		}
	}
	
	private static void GiveKeysToSuccessor() throws IOException {
		succKey = fingerTable.GetSuccessor(myKey);
		succIp = fingerTable.GetIp(succKey);
		succPort = fingerTable.GetPort(succKey);
		String keyMsg = Integer.toString(keyTable.keySet().size());
		for(int i : keyTable.keySet()) {
			int fileKey = i;
			ArrayList<Integer> nodeKeys = keyTable.get(i);
			String file = allFileDetails.get(fileKey);
			for(int i1 : nodeKeys) {
				int nodePort = fingerTable.GetPort(i1);
				String nodeIp = fingerTable.GetIp(i1);
				keyMsg +=  " "+ nodeIp +" "+ Integer.toString(nodePort) +" "+ Integer.toString(fileKey) +" "+ file +"\n";
			}
		}
		keyMsg = "GIVEKY " + String.format("%03d", keyTable.size()) +" "+ keyMsg.substring(0,keyMsg.length()-1);
		keyMsg = String.format("%04d", keyMsg.length()) +" "+ keyMsg;
		if(succKey!=myKey)
			sendrec(keyMsg, succIp, succPort);
	}
	
	public static void Send(String msg, String ip, int port) throws IOException {
		Socket socket = new Socket(ip,port);
		DataOutputStream out = new DataOutputStream(socket.getOutputStream());
		System.out.println("listen.send sending: "+msg);
		out.write(msg.getBytes("UTF-8"));
		System.out.println("sent");
		socket.close();
	}

	public static String sendrec(String Message, String ip, int Port) throws IOException{
		System.out.println();
		logger.log(Level.INFO, "Trying to send the message to Socket address: " + ip + " " + Port);
		System.out.println("Trying to send the message to Socket address: " + ip + " " + Port);
		sock = new Socket(ip, Port);
		DataOutputStream out = new DataOutputStream(sock.getOutputStream());
		DataInputStream in = new DataInputStream(sock.getInputStream());
		System.out.println("main.sendrec sending: "+Message);
		out.write(Message.getBytes("UTF-8"));
	    byte[] rcvdBytes = new byte[10000];
	    in.read(rcvdBytes);
	    String recv = new String(rcvdBytes,0,rcvdBytes.length,"UTF-8");
	    recv= recv.replaceAll("\\p{C}", "");
	    System.out.println("main.sendrec received: "+recv);
	    sock.close();
		logger.log(Level.INFO, "Received the message from Socket address: " + ip + " " + Port);
		return recv;
	}
	
	public static int hash(String msg) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("SHA-1");
		byte[] digest = md.digest(msg.getBytes());
		String s1 = String.format("%8s", Integer.toBinaryString(digest[0] & 0xFF)).replace(' ', '0') +
				String.format("%8s", Integer.toBinaryString(digest[1] & 0xFE)).replace(' ', '0').substring(0,7);
		return Integer.parseInt(s1,2);
	}
	
	public static int resgisterNode() throws IOException, NoSuchAlgorithmException {
		String BS_ip = "198.248.248.130";
		int BS_port = 12000;
		String msg = "REG "+ myIP +" "+ myPort +" "+ "struct";
		msg = String.format("%04d", msg.length()) +" "+ msg;
		String reply = sendrec(msg, BS_ip, BS_port);
		String[] replyExploded = reply.split(" ");
		int noOfRcvdNodes = Integer.parseInt(replyExploded[3]);

		msg = "UPFIN 0 "+ myIP +" "+ myPort +" "+ myKey;
		msg = String.format("%04d", msg.length()) +" "+ msg;
		if (replyExploded[1].equals("REGOK") & replyExploded[2].equals("struct")) {
			if(noOfRcvdNodes>=0 & noOfRcvdNodes<=80) {
				System.out.println("Node registered successfully with bootstrap");
				for(int i = 4; i<replyExploded.length; i+=2) {
					String ip = replyExploded[i];
					String sockAdd = ip+":"+replyExploded[i+1];
					int port = Integer.parseInt(replyExploded[i+1]);
					int key = hash(sockAdd);
					fingerTable.AddEntry(key, ip, port);
					sendrec(msg, ip, port);
				}
				return 0;
			}
			else if (noOfRcvdNodes==9998) {
				System.out.println("Node already registered. Unregistering first");
				deleteNodeBootstrap();
				resgisterNode();
				return 0;
			}
			else if(noOfRcvdNodes==9999) {
				System.out.println("Error in registering. Exiting");
				exit(1);
			}
		}
			return 1;		
	}

	public static void deleteNode() throws IOException {		
		//Unregistering from bootstrap
		String msg = "DEL IPADDRESS "+ myIP +" "+ myPort +" "+ uname;
		msg = String.format("%04d", msg.length()) +" "+ msg;
		sendrec(msg, BS_ip, BS_port);
		System.out.println("Node Deleted Successfully from bootstrap");
		
		//Getting IPLIST from bootstrap
		String reqIpListMsg = String.format("%04d", uname.length()+11) +" GET IPLIST "+ uname;
		String crudeIpList = sendrec(reqIpListMsg, BS_ip, BS_port);
		
		//Unregistering from network
		msg = "UPFIN 1 "+ myIP +" "+ myPort +" "+ myKey;
		msg = String.format("%04d", msg.length()) +" "+ msg;
		String[] ipList = crudeIpList.split(" ");
		for (int i = 6;i<ipList.length;i+=2) {
			sendrec(msg, ipList[i], Integer.parseInt(ipList[i+1]));
		}

		//Giving keys to successor
		GiveKeysToSuccessor();
		exit(0);
	}

	public static void deleteNodeBootstrap() throws IOException {
		String msg = "DEL IPADDRESS "+ myIP +" "+ myPort +" "+ uname;
		msg = String.format("%04d", msg.length()) +" "+ msg;
		sendrec(msg, BS_ip, BS_port);
		System.out.println("Node Deleted Successfully from bootstrap");
	}

	public static void AddResourcesToNetwork() throws NoSuchAlgorithmException, IOException{
		for (String file : nodeResources) {
			int fileKey = hash(file);
			String nodeIp = fingerTable.GetIp(fileKey);
			int nodePort = fingerTable.GetPort(fileKey);
			String msg = "ADD "+ nodeIp +" "+ nodePort +" "+ fileKey +" "+ file;
			msg = String.format("%04d", msg.length()) +" "+ msg;
			sendrec(msg,nodeIp,nodePort);
		}		
	}
	
	public static void exit(int status) {
		logger.log(Level.INFO,"Shutting down the node with status "+ status +".");
		log_file.close();
		sc.close();
		try {
			sock.close();
		} catch (IOException e) {
			System.err.println("ERROR: IO exception in closing socket");
			logger.log(Level.WARNING, "ERROR: IO exception in closing socket");
			e.printStackTrace();
		}
		System.exit(status);
	}
	
	public static void LoadResources(int noOfResources) throws IOException, NoSuchAlgorithmException {
		File file = new File("resources_sp2p.txt");
		FileReader fr = new FileReader(file);						
		BufferedReader br = new BufferedReader(fr);
		StringBuffer sb = new StringBuffer();
		String line;
		while ((line = br.readLine()) != null) {				// Reading the mentioned file.
			if (line.contains("#")) {							// Avoiding the lines starting with #.
				continue;
			}
			sb.append(line);
			sb.append("\n");
		}
		fr.close();												// Closing the file.
		br.close();												// Closing the file reader.

		String[] crudeResources = sb.toString().split("\n");
		ArrayList<String> resources = new ArrayList<String>();
		for(String i : crudeResources) {
			if(i.length()!=0) {                                 // Avoiding lines that are empty.
				resources.add(i.toLowerCase());
			}
		}
		for (String fileName : resources) {
			int key = hash(fileName);
			if(!allFileDetails.keySet().contains(key)) {
				allFileDetails.put(key, fileName);
			}
		}
		
		int i = 0;
		while(i<noOfResources) {
			int rand = (int) (Math.random()*allFileDetails.size());
			if(nodeResources.contains(resources.get(rand))) {
				continue;
			}
			else {
				nodeResources.add(resources.get(rand));
				i++;
			}
		}
	}
}
