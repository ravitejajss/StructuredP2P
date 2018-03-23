import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class structuredPeer {

	private static Logger logger = Logger.getLogger("NodeLog");
	private static String BS_ip;
	private static int BS_port;
	private static String N_ip;
	private static int N_port;
	private static FileHandler log_file;
	private static Socket sock;
	
	public static String uname = "struct";
	public static ConcurrentHashMap<String, String> RT = new ConcurrentHashMap<String, String>();
	
	public static void main(String[] args) {
		
		try {
			
			log_file = new FileHandler("Node.Log");                       //Providing a file to the file handler.
			SimpleFormatter formatter = new SimpleFormatter();            //Logging in Human readable format.
			log_file.setFormatter(formatter);
			logger.addHandler(log_file);
			logger.setUseParentHandlers(false);
			
			BS_ip = args[0];
			BS_port = Integer.parseInt(args[1]);
			N_port = Integer.parseInt(args[2]);
			
			if ((N_port <= 5000 || N_port >= 65535) || (BS_port <= 5000 || BS_port >= 65535 )) {		// Handling Port Exceptions.
				System.out.println("ERROR: Please type an integer in the range of 5001 - 65535 for port number(s).");
				logger.log(Level.WARNING, "User assigned a port number which is out of port ranges.");
				exit(1);
			}
			
			String todo = args[3];
			
			switch (todo) {
			case "REG":
				resgisterNode();
				break;
				
			case "DEL":
				deleteNode();
				break;

			default:
				System.out.println("only REG and DEL are written");
				exit(0);
				break;
			}
			exit(0);
		}
		catch (NumberFormatException nfe) {
			System.err.println("ERROR: Please give integer for port number(s)");
			logger.log(Level.WARNING, "User gave a port number which has non-integers.");
			exit(1);
		} catch (IOException ioe) {
			System.err.println("ERROR: IO exception in main");
			logger.log(Level.WARNING, "ERROR: IO exception in main");
			ioe.printStackTrace();
			exit(1);
		}
	}
	
	public static String sendrec(String Message, String ip, int Port) throws IOException{
		logger.log(Level.INFO, "Sending the message to Socket address: " + ip + " " + Port);
		sock = new Socket(ip, Port);
		DataOutputStream out = new DataOutputStream(sock.getOutputStream());
		DataInputStream in = new DataInputStream(sock.getInputStream());
		out.write(Message.getBytes());
	    System.out.println("msg sent, waiting for reply...");
	    byte[] rcvdBytes = new byte[10000];
	    in.read(rcvdBytes);
	    System.out.println("msg received!\n");
	    String recv = new String(rcvdBytes);
	    System.out.println(recv);
	    sock.close();
		logger.log(Level.INFO, "Received the message from Socket address: " + ip + " " + Port);
		return recv;
	}
	
	public static int hash(String msg) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		MessageDigest md = MessageDigest.getInstance("SHA-1");
		byte[] digest = md.digest(msg.getBytes());
		String hash = Integer.toString((digest[0] & 0xff) ,10);
		System.out.println("hash: "+ hash);
		return Integer.parseInt(hash);
	}
	
	public static int resgisterNode() throws IOException {
		String msg = "REG "+ N_ip +" "+ N_port +" "+ uname;
		msg = String.format("%04d", msg.length()) +" "+ msg;
		String reply = sendrec(msg, BS_ip, BS_port);
		if (reply!=null) {
			String[] rep = reply.split(" ");
			if (rep[1].equals("REGOK")) {				   // Catching the received REGOK and checking for the registration status.
				if (rep[rep.length - 1].equals("9998")) {
					System.out.println("Node already registered.");
					logger.log(Level.WARNING, "User trying to register an already registered node.");
				}
				else if (rep[rep.length - 1].equals("9999")) {
					System.out.println("Error in registering.");
					logger.log(Level.WARNING, "Error in registering.");
					exit(1);
				}
				else if (rep[rep.length - 1].equals("-1")) {
					System.out.println("Unknown REG command.");
					logger.log(Level.WARNING, "Unknown REG command.");
					exit(1);
				}
				else {
					System.out.println("Node Registered Successfully.");
					System.out.println(rep[4]);
					return 0;
				}
			}
		}
			return 1;		
	}

	public static int deleteNode() throws IOException {
		String msg = "DEL IPADDRESS "+ N_ip +" "+ N_port +" "+ uname;
		msg = String.format("%04d", msg.length()) +" "+ msg;
		String reply = sendrec(msg, BS_ip, BS_port);
		System.out.println(reply.split(" ")[5]);
		System.out.println("Node Deleted Successfully");
		return 0;
	}

	public static void exit(int status) {
		logger.log(Level.INFO,"Shutting down the node with status "+ status +".");
		log_file.close();
		try {
			sock.close();
		} catch (IOException e) {
			System.err.println("ERROR: IO exception in exit");
			logger.log(Level.WARNING, "ERROR: IO exception in exit");
			e.printStackTrace();
		}
		System.exit(status);
	}
	
}
