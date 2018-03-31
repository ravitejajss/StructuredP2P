import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class peerListen extends Thread{
	public int answered = 0;
	public int forwarded = 0;
	
	private String myIP;
	private int myPort;
	private int myKey;
	private ConcurrentHashMap<Integer, ArrayList<Integer>> keyTable;
	private FingerTable fingerTable;
	private ServerSocket Sock;
	private ConcurrentHashMap<Integer, String> allFileDetails;
	private Logger logger = Logger.getLogger("ListenLog");		// Declaring the global variables.
	private FileHandler log_file;
	private Socket sock;
	private String uname;
	
	peerListen(int myPort, String myIP, ConcurrentHashMap<Integer, ArrayList<Integer>> keyTable, ConcurrentHashMap<Integer, String> allFileDetails, FingerTable fingerTable, String uname) throws NoSuchAlgorithmException {
		this.myPort = myPort;
		this.myIP = myIP;
		this.keyTable = keyTable;
		this.fingerTable = fingerTable;
		this.allFileDetails = allFileDetails;
		this.uname = uname;
		myKey = structuredPeer.hash(this.myIP+":"+Integer.toString(myPort));
	}
	
	public void run() {
		System.out.println("Entered Listening");
		try {
			Sock = new ServerSocket(myPort);						// binding the socket.
			
			log_file = new FileHandler("Listen.Log");
			SimpleFormatter formatter = new SimpleFormatter();
		    log_file.setFormatter(formatter);
			logger.addHandler(log_file);
			logger.setUseParentHandlers(false);
		} catch (SecurityException e2) {                            // Handling the required exceptions.
			System.err.println("File Handler SecurityException.");
			logger.log(Level.WARNING, "File Handler SecurityException.");
		} catch (IOException e2) {
			System.err.println("IOException Occured. Socket Error.");
			logger.log(Level.WARNING, "IOException Occured. Socket Error.");
		}
		
		while(true) {
			try {
				System.out.println("waiting for connection");
				sock = Sock.accept();
				String rcvd = rcv();
				String[] rcvMsg = rcvd.split(" ");
				String sendMsg;
				int nodeKey;
				
				switch (rcvMsg[1]) {
				case "REG":
					if(rcvMsg[4].equals(uname)) {
						nodeKey = structuredPeer.hash(rcvMsg[2]+":"+rcvMsg[3]);
						fingerTable.AddEntry(nodeKey, rcvMsg[2], Integer.parseInt(rcvMsg[3]));
						sendMsg = "0005 REGOK";
						SendBack(sendMsg);
					}
					break;
					
				case "DEL":
					nodeKey = structuredPeer.hash(rcvMsg[2]+":"+rcvMsg[3]);
					fingerTable.RemoveEntry(nodeKey);
					sendMsg = "0005 DELOK";
					SendBack(sendMsg);
					break;
					
				case "ADD"://format: <length> ADD <IP> <port> <Key> <file name..> <in > <pieces>
				{	//Get all node details of peer that has this file
					nodeKey = structuredPeer.hash(rcvMsg[2]+":"+rcvMsg[3]);
					//Get file key
					int fileKey = Integer.parseInt(rcvMsg[4]);
					//Get file name
					String file = "";
					for (int i = 5;i<rcvMsg.length;i++) {
						file +=  " "+ rcvMsg[i];
					}
					file = file.substring(1);
					System.out.println(file+"|");
					//corruption check
					if(fileKey==structuredPeer.hash(file)) {
						allFileDetails.put(fileKey, file);
						//Add to key table if it is yours
						System.out.println(fingerTable.GetPredecessor(myKey)+"|"+fileKey+"|"+myKey);
						if(fingerTable.GetPredecessor(myKey)<fileKey&fileKey<=myKey||fingerTable.GetPredecessor(myKey)==myKey) {
							System.out.println(fileKey+"|"+nodeKey);
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
							fingerTable.AddDetails(nodeKey, rcvMsg[2], Integer.parseInt(rcvMsg[3]));
						}
						//Forward to responsible peer if not yours
						else if(fileKey>myKey) {
							int respKey = fingerTable.FindNextKey(fileKey);
							System.out.println(respKey+"|"+myKey);
							if(respKey!=myKey)
								structuredPeer.Send(rcvd,fingerTable.GetIp(respKey),fingerTable.GetPort(respKey));
						}
						sendMsg = "0007 ADDOK 0";
						SendBack(sendMsg);
					}
					else {
						System.out.println("Error in listen thread: Got corrupted fileKey in ADD");
						sendMsg = "0007 ADDOK 9998";
						SendBack(sendMsg);
					}
					System.out.println("\nFile     Node - listen thread");
					for (int i : keyTable.keySet()) {
						ArrayList<String> nodes = new ArrayList<String>();
						for(int j: keyTable.get(i))
							nodes.add(String.format("%05d", j));
						System.out.println(String.format("%05d", i) +"   "+ nodes);
					}
				}
					break;
					
				case "UPFIN":
				{
					nodeKey = structuredPeer.hash(rcvMsg[5]);
					if(rcvMsg[2].equals("0")) {
						fingerTable.AddEntry(nodeKey, rcvMsg[3], Integer.parseInt(rcvMsg[4]));
						sendMsg = "0009 UPFINOK 0";
						SendBack(sendMsg);
					}
					else if(rcvMsg[2].equals("1")) {
						fingerTable.RemoveEntry(nodeKey);
						sendMsg = "0009 UPFINOK 0";
						SendBack(sendMsg);
					}
					else {
						sendMsg = "0009 UPFINOK 9999";
						SendBack(sendMsg);
					}
				}
					break;
					
				case "GETKY":
				{
					int key = Integer.parseInt(rcvMsg[2]);
					sendMsg = "";
					int noOfKeys = 0;
					for (int i : keyTable.keySet()) {
						if(i<=key) {
							ArrayList<Integer> nodeKeys = keyTable.get(i);
							for(int j : nodeKeys){
								String file = allFileDetails.get(j);
								String ip = fingerTable.GetIpOfThis(j);
								int port = fingerTable.GetPortOfThis(j);
								sendMsg += " "+ ip +" "+ port +" "+ i +" "+ file;
							}
							noOfKeys++;
						}
					}
					sendMsg = "GETKYOK "+ noOfKeys + sendMsg;
					sendMsg = String.format("%04d", sendMsg.length()) +" "+ sendMsg;
					SendBack(sendMsg);
				}
					break;
				
					
				case "GIVEKY":
				{
					String crudeFileDetails = rcvMsg.toString().substring(16);
					String[] files = crudeFileDetails.split("\n");
					for(int i = 0; i<files.length;i++) {
						String[] fileDetails = files[i].split(" ");
						String ip = fileDetails[0];
						int port = Integer.parseInt(fileDetails[1]);
						nodeKey = structuredPeer.hash(ip +":"+ fileDetails[1]);
						int fileKey = Integer.parseInt(fileDetails[2]);
						String file = "";
						for(int j = 3; j<fileDetails.length; j++) {
							file+= " "+ fileDetails[i];
						}
						allFileDetails.put(fileKey, file);
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
						fingerTable.AddDetails(nodeKey, ip, port);
					}
					sendMsg = "0010 GIVEKYOK 0";
					SendBack(sendMsg);
				}
					break;
				
					
				case "SER"://format: <length> SER <IP> <port> <Key>
				{
					int fileKey = Integer.parseInt(rcvMsg[4]);
					if(keyTable.keySet().contains(fileKey)) {
						String SERip = rcvMsg[2];
						int SERport = Integer.parseInt(rcvMsg[3]);
						String file = allFileDetails.get(fileKey);
						int no = 0;
						sendMsg = "";
						ArrayList<Integer> nodeKeys = keyTable.get(fileKey);
						for(int i : nodeKeys) {
							String ip = fingerTable.GetIpOfThis(i);
							int port = fingerTable.GetPortOfThis(i);
							sendMsg = ip +" "+ port +" "+ file +"\n";
							no++;
						}
						sendMsg = "SEROK "+ String.format("%03d", no) +" "+ sendMsg.substring(0, sendMsg.length()-1);
						sendMsg = String.format("%04d", sendMsg.length()) +" "+ sendMsg;
						structuredPeer.Send(sendMsg, SERip, SERport);
						answered++;
					}
					else
					{
						int respKey = fingerTable.FindNextKey(fileKey);
						String ip = fingerTable.GetIp(respKey);
						int port = fingerTable.GetPort(respKey);
						structuredPeer.Send(rcvd, ip, port);
						forwarded++;
					}
				}
					break;
				
				case "SEROK":
				{
					int noOfKeys = Integer.parseInt(rcvMsg[2]);
					if(noOfKeys>0) 
					{
						System.out.println("Search successfully comleted. Results are as shown below\n");
						String crudeFileDetails = rcvMsg.toString().substring(15);
						String[] files = crudeFileDetails.split("\n");
						for(int i = 0; i<files.length;i++) {
							String[] fileDetails = files[i].split(" ");
							String ip = fileDetails[0];
							int port = Integer.parseInt(fileDetails[1]);
							String file = "";
							for(int j = 2; j<fileDetails.length; j++) {
								file+= " "+ fileDetails[i];
							}
							System.out.println("Found '"+ file +"' at IP: "+ ip +" Port: "+ port +"\n");
						}
						structuredPeer.isSearchComplete=true;
					}
				}
					break;
					
				default:
					break;
				}
			sock.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				System.err.println("Error: No Such Algorithm");
			}
		}
	}

	private String rcv() throws IOException {
		DataInputStream in = new DataInputStream(sock.getInputStream());
		byte[] rcvdBytes = new byte[10000];
	    in.read(rcvdBytes);
	    String recv = new String(rcvdBytes,0,rcvdBytes.length,"UTF-8");
	    recv = recv.replaceAll("\\p{C}", "");
	    System.out.println("listen.rcv received: "+recv);
		return recv;
	}
	
	private void SendBack(String Message) throws IOException {
		DataOutputStream out = new DataOutputStream(sock.getOutputStream());
		System.out.println("listen.sendBack sending: "+Message);
		out.write(Message.getBytes("UTF-8"));
		System.out.println("sent");
	}
}
