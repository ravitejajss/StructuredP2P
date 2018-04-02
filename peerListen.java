import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class peerListen extends Thread{
	public int answered = 0;
	public int forwarded = 0;
	public int received = 0;
	
	private String myIP;
	private int myPort;
	private int myKey;
	private ConcurrentHashMap<Integer, ArrayList<Integer>> keyTable;
	private FingerTable fingerTable;
	private ServerSocket Sock;
	private ConcurrentHashMap<Integer, String> allFileDetails;
	private Logger logger = Logger.getLogger("ListenLog");
	FileHandler log_file;
	private ArrayList<String> HandledMsgs = new ArrayList<String>();
	private Socket sock;
	
	peerListen(int myPort, String myIP, ConcurrentHashMap<Integer, ArrayList<Integer>> keyTable, ConcurrentHashMap<Integer, String> allFileDetails, FingerTable fingerTable) throws NoSuchAlgorithmException {
		this.myPort = myPort;
		this.myIP = myIP;
		this.keyTable = keyTable;
		this.fingerTable = fingerTable;
		this.allFileDetails = allFileDetails;
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
				sock = Sock.accept();
				String rcvd = rcv();
				String[] rcvMsg = rcvd.split(" ");
				String sendMsg;
				
				switch (rcvMsg[1]) {
				case "ADD":
				{	//Get all node details of peer that has this file
					int nodeKey = structuredPeer.hash(rcvMsg[2]+":"+rcvMsg[3]);
					//Get file key
					int fileKey = Integer.parseInt(rcvMsg[4]);
					//Get file name
					String file = "";
					for (int i = 5;i<rcvMsg.length;i++) {
						file +=  " "+ rcvMsg[i];
					}
					file = file.substring(1);
					//corruption check
					if(fileKey==structuredPeer.hash(file)) {
						int predKey = fingerTable.GetPredecessor(myKey);
						
						//Add to key table if it is yours
						if((predKey<fileKey&fileKey<=myKey)||predKey==myKey||(fileKey<=myKey||fileKey>predKey)) {
							if(keyTable.containsKey(fileKey)) {
								ArrayList<Integer> nodesHasFile = keyTable.get(fileKey);
								if(!nodesHasFile.contains(nodeKey))
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
						else {
							int respKey = fingerTable.GetSuccessor(fileKey);
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
				}
					break;
					
				case "UPFIN":
				{
					int nodeKey = Integer.parseInt(rcvMsg[5]);
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
					sendMsg = " ";
					int noOfKeys = 0;
					for (int i : keyTable.keySet()) {
						if(i<=key) {
							ArrayList<Integer> nodeKeys = keyTable.get(i);
							for(int j : nodeKeys){
								String file = allFileDetails.get(i);
								String ip = fingerTable.GetIp(j);
								int port = fingerTable.GetPort(j);
								sendMsg += ip +" "+ port +" "+ i +" "+ file +"#";
							}
							noOfKeys++;
						}
					}
					sendMsg = "GETKYOK "+ String.format("%03d", noOfKeys) + sendMsg.substring(0,sendMsg.length()-1);
					sendMsg = String.format("%04d", sendMsg.length()) +" "+ sendMsg;
					SendBack(sendMsg);
				}
					break;
				
					
				case "GIVEKY":
				{
					String crudeFileDetails = rcvd.substring(16);
					String[] files = crudeFileDetails.split("#");
					for(int i = 0; i<files.length;i++) {
						String[] fileDetails = files[i].split(" ");
						String ip = fileDetails[0];
						int port = Integer.parseInt(fileDetails[1]);
						int nodeKey = structuredPeer.hash(ip +":"+ fileDetails[1]);
						int fileKey = Integer.parseInt(fileDetails[2]);
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
					received++;
					String entry = rcvd.substring(0, 9) +" "+ rcvd.substring(11);
					int hopCount = Integer.parseInt(rcvMsg[2]);
					String SERip = rcvMsg[4];
					int SERport = Integer.parseInt(rcvMsg[5]);
					if(hopCount<=0) {
						sendMsg = "0008 SEROK -1";
						structuredPeer.Send(sendMsg, SERip, SERport);
						break;
					}
					int fileKey = Integer.parseInt(rcvMsg[6]);
					if(keyTable.keySet().contains(fileKey)) {
						String file = allFileDetails.get(fileKey);
						int no = 0;
						sendMsg = "";
						ArrayList<Integer> nodeKeys = keyTable.get(fileKey);
						for(int i : nodeKeys) {
							String ip = fingerTable.GetIp(i);
							int port = fingerTable.GetPort(i);
							sendMsg = ip +" "+ port +" "+ file +"\n";
							no++;
						}
						sendMsg = "SEROK "+ String.format("%02d", (hopCount-1)) +" "+ rcvMsg[3] +" "+ String.format("%03d", no) +" "+ sendMsg.substring(0, sendMsg.length()-1);
						sendMsg = String.format("%04d", sendMsg.length()) +" "+ sendMsg;
						structuredPeer.Send(sendMsg, SERip, SERport);
						answered++;
					}
					else
					{
						if(!HandledMsgs.contains(entry)) {
							int respKey = fingerTable.GetResp(fileKey);
							String ip = fingerTable.GetIp(respKey);
							int port = fingerTable.GetPort(respKey);
							if(respKey!=myKey) {
								HandledMsgs.add(entry);
								rcvd = rcvd.substring(0, 9) + String.format("%02d", (hopCount-1)) + rcvd.substring(11);
								structuredPeer.Send(rcvd, ip, port);
							}
							forwarded++;
						}
					}
				}
					break;
				
				case "SEROK":
				{
					if(Integer.parseInt(rcvMsg[2])==-1) {
						System.out.println("Search successfully comleted. File not present in the network\n");
						structuredPeer.isSearchComplete=true;
						break;
					}
					int noOfKeys = Integer.parseInt(rcvMsg[4]);
					if(noOfKeys>0) 
					{
						long timeNow = System.currentTimeMillis();
						System.out.println("Search successfully comleted. Results are as shown below\n");
						int netHops = 7 - Integer.parseInt(rcvMsg[2]);
						int netTime = (int) (timeNow - Long.parseLong(rcvMsg[3]));
						Timestamp time = new Timestamp(timeNow);
						String crudeFileDetails = "";
						for(int i = 5; i< rcvMsg.length; i++)
							crudeFileDetails += " "+ rcvMsg[i];
						crudeFileDetails = crudeFileDetails.substring(1);
						String[] files = crudeFileDetails.split("\n");
						for(int i = 0; i<files.length;i++) {
							String[] fileDetails = files[i].split(" ");
							String ip = fileDetails[0];
							int port = Integer.parseInt(fileDetails[1]);
							String file = "";
							for(int j = 2; j<fileDetails.length; j++) {
								file+= " "+ fileDetails[j];
							}
							file = file.substring(1);
							System.out.println("Found '"+ file +"' at IP: "+ ip +" Port: "+ port + " hops: " + Integer.toString(netHops) + " latency: " + Double.toString(netTime) +"\n");
							String write = "Found '"+ file +"' at IP: "+ ip +" Port: "+ port + "\t" + Integer.toString(netHops) + "\t" + Double.toString(netTime) + "\t" + time + "\n";
							BufferedWriter out = null;
							FileWriter fr = new FileWriter("results.txt",true);						
							out = new BufferedWriter(fr);
							out.write(write);
							out.close();
						}
						structuredPeer.isSearchComplete=true;
					}
				}
					break;
					
				default:
					System.out.println("Error: listen thread received unknown message");
					break;
				}
			sock.close();
			} catch (IOException e) {
				System.err.println("Error: IO exception in listen thread.");
				e.printStackTrace();
				structuredPeer.exit(1);
			} catch (NoSuchAlgorithmException e) {
				System.err.println("Error: No Such Algorithm");
				e.printStackTrace();
				structuredPeer.exit(1);
			}
		}
	}

	private String rcv() throws IOException {
		DataInputStream in = new DataInputStream(sock.getInputStream());
		byte[] rcvdBytes = new byte[10000];
	    in.read(rcvdBytes);
	    String recv = new String(rcvdBytes,0,rcvdBytes.length,"UTF-8");
	    recv = recv.replaceAll("\\p{C}", "");
		return recv;
	}
	
	private void SendBack(String Message) throws IOException {
		DataOutputStream out = new DataOutputStream(sock.getOutputStream());
		out.write(Message.getBytes("UTF-8"));
	}
	
}
