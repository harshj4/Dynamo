package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.StrictMode;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	private String thisNode;
	private String thisPort;
	private boolean isReconciling = false;
	private ArrayList<Integer> nodeList = new ArrayList<Integer>();
	private ArrayList<Integer> portList = new ArrayList<Integer>();
	private ArrayList<String> peerList = new ArrayList<String>();
	private HashMap<String,Integer> peerMap = new HashMap<String, Integer>();
	private HashMap<String,Integer> versionMap = new HashMap<String, Integer>();
	private ArrayList<String> ownKeys = new ArrayList<String>();
	private ArrayList<String> parentKeys = new ArrayList<String>();
	private ArrayList<String> grandParentKeys = new ArrayList<String>();
	private Uri uri;
	private SharedPreferences serverSP;
	private ArrayList<Integer> readQuorum = new ArrayList<Integer>();
	private ArrayList<Integer> writeQuorum = new ArrayList<Integer>();
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		serverSP = getContext().getSharedPreferences("storage", Context.MODE_PRIVATE);
		SharedPreferences.Editor editor = serverSP.edit();
		editor.clear();
		editor.remove(selection);
		editor.commit();
		ownKeys.clear();
		parentKeys.clear();
		grandParentKeys.clear();
		versionMap.clear();
		for(int node:nodeList){
			sendMsg("DALL:",node);
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		while(true){
			if(!isReconciling){
				Log.d("Breaking out of ","infinite while");
				break;
			}
		}
		serverSP = getContext().getSharedPreferences("storage", Context.MODE_PRIVATE);
		int masterNode = 0;
		try {
			String keyToPut = null;
			keyToPut = (String)values.get("key");
			Log.d("keyToPut",keyToPut);
			String valueToPut = (String) values.get("value");
			String keyHash = genHash(keyToPut);
			if(keyHash.compareTo(peerList.get(peerList.size()-1)) > 0){
				masterNode = peerMap.get(peerList.get(0));
			}
			else{
				for(String peer:peerList){
					if(keyHash.compareTo(peer) < 0){
						masterNode = peerMap.get(peer);
						Log.d("Master node is",Integer.toString(masterNode)+" for "+keyToPut);
						break;
					}
				}
			}
			int masterIndex = peerList.indexOf(genHash(Integer.toString(masterNode)));
			Log.d("Master index is",Integer.toString(masterIndex)+ " for "+Integer.toString(masterIndex));
			int count = 0;
			while(count < 3){
				int target = peerMap.get(peerList.get(masterIndex));
				String insertMsg = "INSERT:"+keyToPut+":"+valueToPut+":"+Integer.toString(count);
				sendMsg(insertMsg,target);
				masterIndex++;
				count++;
				if(masterIndex > peerList.size()-1){
					masterIndex = 0;
				}
			}



		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		StrictMode.ThreadPolicy policy = new StrictMode.ThreadPolicy.Builder().permitAll().build();

		StrictMode.setThreadPolicy(policy);
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		thisNode = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		thisPort = String.valueOf((Integer.parseInt(thisNode) * 2));
		int temp = 5554;
		for(int i=0;i<5;i++){
			nodeList.add(temp);
			portList.add(temp * 2);
			try {
				peerList.add(genHash(Integer.toString(temp)));
				peerMap.put(genHash(Integer.toString(temp)),temp);
				temp = temp + 2;
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		Collections.sort(peerList);
		Log.d("PeerList is:",peerList.toString());
		Log.d("NodeList is",nodeList.toString());

		uri = buildUri("content","edu.buffalo.cse.cse486586.simpledynamo.provider");
		ServerSocket serverSocket = null;

		try {
			String thisHash = genHash(thisNode);
			int selfIndex = peerList.indexOf(thisHash);
			int targetIndex;
			if(selfIndex == 0){
				targetIndex = peerList.size()-1;
			}
			else{
				targetIndex = selfIndex - 1;
			}
			int count = 0;
			while(count < 2){
				int target = peerMap.get(peerList.get(targetIndex));
				String reconMsg = "RECON:0";
				Socket sock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), target * 2);
				DataOutputStream rdos = new DataOutputStream(sock.getOutputStream());
				DataInputStream rdis = new DataInputStream(sock.getInputStream());
				rdos.writeUTF(reconMsg);
				String reconReply = null;
				rdos.flush();
				try{
					reconReply = rdis.readUTF();
				}catch(SocketException eof){
					targetIndex--;
					count++;
					if(targetIndex < 0){
						targetIndex = peerList.size()-1;
					}
					continue;
				}catch(IOException io){
					targetIndex--;
					count++;
					if(targetIndex < 0){
						targetIndex = peerList.size()-1;
					}
					continue;
				}



				Log.d("RECON reply is",reconReply);
				if(reconReply.equals("NULL")){
					Log.d("This is the ","beginning of time.");
					break;
				}
				Log.d("Starting","Reconciliation");
				isReconciling = true;
				String[] pairs = reconReply.split(":");
				for(String pair:pairs){
					if(pair.equals("LOSTDATA")){
						continue;
					}
					String[] entries = pair.split("#");
					versionMap.put(entries[0],Integer.parseInt(entries[2]));
					ContentValues cv = new ContentValues();
					cv.put("key",entries[0]);
					cv.put("value",entries[1]);
					localInsert(uri,cv);
					if(count == 0){
						parentKeys.add(entries[0]);
					}
					else{
						grandParentKeys.add(entries[0]);
					}
				}
				targetIndex--;
				count++;
				if(targetIndex < 0){
					targetIndex = peerList.size()-1;
				}
			}

			if(versionMap.size()!=0){
				Log.d("Reconciling","from successor");
				if(selfIndex == peerList.size()-1){
					targetIndex = 0;
				}
				else{
					targetIndex = selfIndex + 1;
				}

					int target = peerMap.get(peerList.get(targetIndex));
					Socket sock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), target * 2);
					DataInputStream rdis = new DataInputStream(sock.getInputStream());
					DataOutputStream rdos = new DataOutputStream(sock.getOutputStream());
					rdos.writeUTF("RECON:1");
					rdos.flush();
					String reconReply = null;
					try{
						reconReply = rdis.readUTF();
					}catch(SocketException eof){
						Log.d("EOF ","caught");
					}

					Log.d("RECON reply is ",reconReply);
					String[] pairs = reconReply.split(":");
					for(String pair:pairs){
						if(pair.equals("LOSTDATA")){
							continue;
						}
						String[] entries = pair.split("#");
						versionMap.put(entries[0],Integer.parseInt(entries[2]));
						ContentValues cv = new ContentValues();
						cv.put("key",entries[0]);
						cv.put("value",entries[1]);
						localInsert(uri,cv);
						ownKeys.add(entries[0]);

					}

			}


		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			serverSocket = new ServerSocket(10000);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		} catch (IOException e) {
			e.printStackTrace();
		}
		isReconciling = false;
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		while(true){
			if(!isReconciling){
				Log.d("Breaking out of ","infinite while");
				break;
			}
		}
		Cursor outCursor = new MatrixCursor(new String[] {"key","value"});
		try {
			if(selection.equals("@")){
				Map tempMap=null;
				if(serverSP.getAll() != null){
					tempMap = serverSP.getAll();

				}
				else{
					return outCursor;
				}
				for(Object item:tempMap.keySet()){
					((MatrixCursor) outCursor).addRow(new Object[]{(String)item,tempMap.get(item)});
				}
				return outCursor;
			}
			else if(selection.equals("*")){
				for(int node:nodeList){
					String starMsg = "ALL:";
					Socket sock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), node * 2);
					DataOutputStream dos = new DataOutputStream(sock.getOutputStream());
					DataInputStream dis = new DataInputStream(sock.getInputStream());
					dos.writeUTF(starMsg);
					dos.flush();
					Thread.sleep(1);
					String allKeys = null;
					try{
						allKeys = dis.readUTF();
					}catch(SocketException eof){
						Log.d("Catching","EOF in query *");
						continue;
					}catch(IOException io){
						Log.d("Catching IOE","in star query");
						continue;
					}
					Log.d("ALL keys are",allKeys);
					String[] entries = allKeys.split(":");
					for(int i=1;i<entries.length;i++){
						String entry = entries[i];
						String[] pair = entry.split("#");
						String key = pair[0];
						String val = pair[1];
						((MatrixCursor) outCursor).addRow(new Object[]{key,val});
					}
				}
				return outCursor;

			}
			else{
				int masterNode = 0;
				String selectionHash = genHash(selection);
				if(selectionHash.compareTo(peerList.get(peerList.size()-1)) > 0){
					masterNode = peerMap.get(peerList.get(0));
				}
				else{
					for(String peer:peerList){
						if(selectionHash.compareTo(peer) < 0){
							masterNode = peerMap.get(peer);
							break;
						}
					}
				}
				int masterIndex = peerList.indexOf(genHash(Integer.toString(masterNode)));
				int keyVersion = 0;
				String key = "";
				String value = "";
				int count = 0;
				while( count < 3){

					int target = peerMap.get(peerList.get(masterIndex));
					String insertMsg = "QUERY:"+selection;
					Socket sock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), target*2);
					Log.d("Sending query to",Integer.toString(target));
					DataOutputStream qdos = new DataOutputStream(sock.getOutputStream());
					DataInputStream qdis = new DataInputStream(sock.getInputStream());
					qdos.writeUTF(insertMsg);
					qdos.flush();
					Thread.sleep(1);
					String qack = null;
					try{
						qack = qdis.readUTF();
					}catch(SocketException eof){

						Log.d("Catching EOF","in single query");
						masterIndex++;
						count++;
						if(masterIndex > peerList.size()-1){
							masterIndex = 0;
						}
						continue;
					} catch(IOException io){
						Log.d("Catching IOE","in single query");
						masterIndex++;
						count++;
						if(masterIndex > peerList.size()-1){
							masterIndex = 0;
						}
						continue;
					}
					String[] qresp = qack.split(":");
					Log.d("QRESP is: ",qack);
					if(Integer.parseInt(qresp[3]) > keyVersion){
						keyVersion = Integer.parseInt(qresp[3]);
						key = qresp[1];
						value = qresp[2];
						Log.d("Overwritting "+key,value + " : "+ Integer.toString(keyVersion));
					}
					masterIndex++;
					count++;
					if(masterIndex > peerList.size()-1){
						masterIndex = 0;
					}
				}

				((MatrixCursor) outCursor).addRow(new Object[]{key,value});
				while(outCursor.moveToNext()){
					Log.d("outCursor has",outCursor.getString(0));
				}
			}


		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			Log.d("Catching","IOException");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return outCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Socket listeningSocket = null;
			Log.d("Inside","ServerTask");
			String receivedString;
			Cursor responseCursor;
			uri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
			while(true){

				try {
					Log.d("Before","Accept");
					listeningSocket = serverSocket.accept();
					listeningSocket.setTcpNoDelay(true);
					Log.d("After","Accept");
					DataInputStream dis = new DataInputStream(listeningSocket.getInputStream());
					DataOutputStream dos = new DataOutputStream(listeningSocket.getOutputStream());
					receivedString = (String) dis.readUTF();
					Log.d("Received string is",receivedString);
					String[] splits = receivedString.split(":");
					if(splits[0].equals("INSERT")){
						ContentValues cv = new ContentValues();
						String keyToPut = splits[1];
						cv.put("key",keyToPut);
						cv.put("value",splits[2]);
						localInsert(uri,cv);
						if(!versionMap.containsKey(keyToPut)){
							versionMap.put(keyToPut, 1);
						}
						else{
							int currentVersion = versionMap.get(keyToPut);
							Log.d("Updating version for"+ keyToPut, "to "+ Integer.toString(currentVersion + 1));
							versionMap.put(keyToPut, currentVersion + 1);
						}
						if(splits[3].equals("0") && !ownKeys.contains(keyToPut)){
							Log.d("Adding to own",keyToPut);
							ownKeys.add(keyToPut);
						}
						else if(splits[3].equals("1") && !parentKeys.contains(keyToPut)){
							Log.d("Adding to parent",keyToPut);
							parentKeys.add(keyToPut);
						}
						else if(splits[3].equals("2") && !grandParentKeys.contains(keyToPut)){
							Log.d("Adding to grandparent",keyToPut);
							grandParentKeys.add(keyToPut);
						}
					}
					else if(splits[0].equals("ALL")){
						Cursor atCursor = query(uri,null,"@",null,null);
						String response = "QRESP";
						while(atCursor.moveToNext()){
							response = response + ":" +atCursor.getString(0)+"#"+atCursor.getString(1);
						}
						Log.d("Sending @ resopnse: ",response);
						dos.writeUTF(response);
						dos.flush();
					}
					else if(splits[0].equals("QUERY")){
						Thread.sleep(1);
						Cursor c = localQuery(uri,splits[1]);
						String response = "QRESP";
						if(c != null){

							while(c.moveToNext()){
								response = response + ":" +c.getString(0)+":"+c.getString(1) + ":" + Integer.toString(versionMap.get(splits[1]));
							}

						}
						else{
							response = response + ":NA:NA:0";
						}
						dos.writeUTF(response);
						dos.flush();
					}
					else if(splits[0].equals("RECON")){
						String respString = "LOSTDATA";
						if(splits[1].equals("0")){
							if(ownKeys.size()==0){
								dos.writeUTF("NULL");
								dos.flush();
								continue;
							}
							for(String k: ownKeys){
								Cursor localCursor = localQuery(uri, k);
								localCursor.moveToFirst();
								respString = respString + ":" + k + "#" + localCursor.getString(1) + "#" + versionMap.get(k);
							}
							dos.writeUTF(respString);
							dos.flush();
						}
						else{
							for(String k: parentKeys){
								Cursor localCursor = localQuery(uri, k);
								localCursor.moveToNext();
								respString = respString + ":" + k + "#" + localCursor.getString(1) + "#" + versionMap.get(k);
							}
							dos.writeUTF(respString);
							dos.flush();
						}
					}else if(splits[0].equals("DALL")){
						serverSP = getContext().getSharedPreferences("storage", Context.MODE_PRIVATE);
						SharedPreferences.Editor editor = serverSP.edit();
						editor.clear();
						editor.commit();
						ownKeys.clear();
						parentKeys.clear();
						grandParentKeys.clear();
						versionMap.clear();
					}
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
//			return null;
		}
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public void sendMsg(String msg, int dest){
		Log.d("Sending message: ", msg + " TO "+ Integer.toString(dest));
		new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,Integer.toString(dest));
	}

	private class clientTask extends AsyncTask<String,Void,Void>{
		@Override
		protected Void doInBackground(String... strings) {
			String msgToSend = strings[0];
			String destination = strings[1];
			try {
				Log.d("Sending to",destination);
				Log.d("msgToSend",msgToSend);
				Socket sock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(destination) * 2);
				DataOutputStream dos = new DataOutputStream(sock.getOutputStream());
				dos.writeUTF(msgToSend);
				dos.flush();
				Thread.sleep(1);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	void findQuorums(int masterNode){
		if(masterNode == 11120){
			writeQuorum.add(11120);
			writeQuorum.add(11124);
			readQuorum.add(11124);
			readQuorum.add(11108);
		}
		else if(masterNode == 11124){
			writeQuorum.add(11124);
			writeQuorum.add(11108);
			readQuorum.add(11108);
			readQuorum.add(11112);
		}
		else{
			writeQuorum.add(masterNode);
			writeQuorum.add(masterNode + 4);
			readQuorum.add(masterNode + 4);
			readQuorum.add(masterNode + 8);
		}
		Log.d("writeQuorum:",writeQuorum.toString());
		Log.d("readQuorum:",readQuorum.toString());
	}

	private void localInsert(Uri uri, ContentValues values){
		String keyToPut = (String)values.get("key");
		String valueToPut = (String) values.get("value");
		serverSP = getContext().getSharedPreferences("storage", Context.MODE_PRIVATE);
		SharedPreferences.Editor spEditor = serverSP.edit();
		spEditor.putString(keyToPut,valueToPut);
		spEditor.commit();

	}

	private Cursor localQuery(Uri uri,String selection){
		Cursor tempCursor = new MatrixCursor(new String[] {"key","value"});
		serverSP = getContext().getSharedPreferences("storage", Context.MODE_PRIVATE);
		String extractedValue = serverSP.getString(selection, "-1");
		if(!extractedValue.equals("-1")){
			Log.d("Extracted value for " + selection, extractedValue);
			((MatrixCursor) tempCursor).addRow(new Object[]{selection, extractedValue});
			return tempCursor;
		}
		return null;
	}
}
