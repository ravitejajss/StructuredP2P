import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class peerListen extends Thread{
	
	private String myIP;
	private int myPort;
	private int myKey;
	private ConcurrentHashMap<Integer, Integer> keyTable = new ConcurrentHashMap<Integer,Integer>();
	private FingerTable fingerTable;
	private ServerSocket Sock;
	private Logger logger = Logger.getLogger("ListenLog");		// Declaring the global variables.
	private FileHandler log_file;
	private Socket sock;
	
	peerListen(int myPort, String myIP, ConcurrentHashMap<Integer, Integer> keyTable, FingerTable fingerTable) throws NoSuchAlgorithmException {
		this.myPort = myPort;
		this.myIP = myIP;
		this.keyTable = keyTable;
		this.fingerTable = fingerTable;
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
				String[] rcvd = rcv();
				String[] rcvMsg = rcvd[0].split(" ");
				String ip = rcvd[1];
				int port = Integer.parseInt(rcvd[2]);
				String sockAdd = ip+":"+port;
				int nodeKey = structuredPeer.hash(sockAdd);
				String sendMsg;
				
				switch (rcvMsg[1]) {
				case "REG":
					fingerTable.AddEntry(nodeKey, ip, port);
					sendMsg = "0005 REGOK";
					SendBack(sendMsg);
					break;
					
				case "DEL":
					fingerTable.RemoveEntry(nodeKey);
					sendMsg = "0005 DELOK";
					SendBack(sendMsg);
					break;
					
				case "ADD":
					int fileKey = Integer.parseInt(rcvMsg[4]);
					String file = rcvMsg[5];
					if(fileKey==structuredPeer.hash(file)) {
						if(fingerTable.FindKey(fingerTable.GetPredecessor())<fileKey&fileKey<=myKey)
							keyTable.put(fileKey, nodeKey);
						else if(fileKey>myKey) {
							int respKey = fingerTable.FindKey(fileKey);
							Send(rcvd[0],fingerTable.GetIp(respKey),fingerTable.GetPort(respKey));
						}
					}
					break;
					
				case "UPFIN":
					
					break;
					
				case "GETKY":
	
					break;
					
				case "GIVEKY":
					
					break;
					
				case "SER":
					
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
	
	private void Send(String msg, String ip, int port) throws IOException {
		Socket socket = new Socket(ip,port);
		DataOutputStream out = new DataOutputStream(socket.getOutputStream());
		out.write(msg.getBytes("UTF-8"));
		socket.close();
	}

	private String[] rcv() throws IOException {
		DataInputStream in = new DataInputStream(sock.getInputStream());
		byte[] rcvdBytes = new byte[10000];
	    in.read(rcvdBytes);
	    String recv = new String(rcvdBytes,0,rcvdBytes.length,"UTF-8").replaceAll("\\p{C}", "");
	    System.out.println(recv);
	    String[] rcvd = {recv, sock.getInetAddress().getHostAddress(), Integer.toString(sock.getPort())};
		return rcvd;
	}
	
	private void SendBack(String Message) throws IOException {
		DataOutputStream out = new DataOutputStream(sock.getOutputStream());
		out.write(Message.getBytes("UTF-8"));
	}
}
