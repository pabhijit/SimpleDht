package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    private static final String TAG = SimpleDhtProvider.class.getSimpleName();
    private final Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    private static final int SERVER_PORT = 10000;
    private static final String JOIN_PORT = "11108";
    private static final String JOIN_STR = "join";
    private static final String UPDATE_STR = "updatee";
    private static final String DELETE_STR = "deletee";
    private static final String QUERY_STR_GLOBAL = "queryGlobal";
    private static final String QUERY_STR_LOCAL = "queryLocal";
    private static final String INSERT_STR = "insertt";
    private static final String FOUND_STR = "foundd";
    private static Set<String> ring = new HashSet<String>();
    private static String[] cols = new String[] { "key", "value" };
    private static String globalQuery = "";
    private static MatrixCursor cursor = null;
    private static boolean checkQuery = false;
    private static Node thisNode = null;
    private static boolean deleted = false;
    private static String valueFound = null;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        int retVal = 0;
        if( selection.matches("\"*\"") && !deleted ){
            retVal = deleteLocalPairs();
            deleted = true;
            new ClientTask().executeOnExecutor( AsyncTask.SERIAL_EXECUTOR, DELETE_STR, thisNode.getSuccessor().getMyId() );
            Log.d(TAG, "Number of files deleted is : "+ retVal);
        } else if( selection.matches("\"@\"")){
            retVal = deleteLocalPairs();
            Log.e(TAG, "Number of files deleted is : "+ retVal);
        } else {
            deleteLocalPairs();
        }
        return retVal;
    }

    private int deleteLocalPairs() {

        int retVal = 0;
        try{
            Log.d(TAG, "Deleting the local files");
            File file = new File(getContext().getFilesDir().getAbsolutePath());

            for(File toBeDeleted : file.listFiles()){
                toBeDeleted.delete();
                retVal += 1;
            }

        }catch(Exception e){
            Log.e(TAG, "deleteLocal: File delete failed");
        }
        return retVal;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String filename = null;
        String fileHashed = null;
        String content = null;
        FileOutputStream outputStream;

        try {
            deleted = false;
            for(String key: values.keySet()) {
                if(key.equals("key")){
                    filename = String.valueOf(values.get(key));
                    fileHashed = genHash(filename);
                } else {
                    content = String.valueOf(values.get(key));
                }
            }
            Log.d(TAG, "the predecessor of "+thisNode.getMyId() +" is "+ thisNode.getPredecessor().getMyId());
            Log.d(TAG, "the successor of "+thisNode.getMyId() +" is "+ thisNode.getSuccessor().getMyId());
            if( ring.size() == 1 || (fileHashed.compareTo(thisNode.getPredecessor().getMyHash()) > 0 && fileHashed.compareTo(thisNode.getMyHash()) <= 0) ||
                    (thisNode.getMyHash().equals(genHashPort(thisNode.getSmallest())) &&
                            ( fileHashed.compareTo(thisNode.getMyHash()) <= 0 || fileHashed.compareTo(genHashPort(thisNode.getLargest())) > 0 ))) {

                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(content.getBytes());
                outputStream.close();
            } else if( thisNode.getMyHash().equals( genHashPort(thisNode.getLargest()) ) ) {
                new ClientTask().executeOnExecutor( AsyncTask.SERIAL_EXECUTOR, INSERT_STR, thisNode.getSmallest(), filename + "%%" + content );
            } else {
                new ClientTask().executeOnExecutor( AsyncTask.SERIAL_EXECUTOR, INSERT_STR, thisNode.getSuccessor().getMyId(), filename + "%%" + content );
            }

        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }

        Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {
            try {
                Log.d(TAG, "onCreate: Creating a ServerSocket");
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (IOException e) {
                Log.e(TAG, "onCreate: Can't create a ServerSocket");
                return false;
            }
            thisNode = new Node();
            String myPort = getPortNumber();
            prepareThisNode(myPort,myPort,myPort);

            ring.add(JOIN_PORT);
            Log.d(TAG, "onCreate : Calling client task");
            if(!myPort.equals(JOIN_PORT)) {
                new ClientTask().executeOnExecutor( AsyncTask.SERIAL_EXECUTOR,JOIN_STR, JOIN_PORT, myPort );
            }


        }catch(Exception e){
            Log.e(TAG, "onCreate:Exception in SimpleDhtProvider");
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        String line = null;
        cursor = new MatrixCursor(cols);
        String value = null;
        FileInputStream fis = null;
        BufferedReader reader = null;
        String hashQuery = null;
        String result = null;
        Log.e(TAG, "The selection parameter is : "+ selection);
        String initiator = selectionArgs==null?getPortNumber():selectionArgs[0];
        Log.e(TAG, "The initiator is : "+ initiator);
        try {

            if( selection.contains("*") ){

                if(selectionArgs != null){
                    result = selectionArgs[1];
                } else {
                    result = "";
                }
                for (String file : getContext().fileList()) {
                    fis = getContext().openFileInput(file);
                    reader = new BufferedReader(new InputStreamReader(fis));

                    while ((line = reader.readLine()) != null) {
                        value = line;
                    }
                    result = result + file + "%" + value.toString() + "%";
                    Log.d(TAG, "query : The global query is : "+ result);
                }
                if (thisNode.getMyHash().equals(genHashPort(thisNode.getLargest())) && !thisNode.getMyHash().equals(genHashPort(thisNode.getSmallest()))) {

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERY_STR_GLOBAL, thisNode.getSmallest(), initiator, result);
                    if(initiator.equals(getPortNumber())) {
                        while (!checkQuery) {

                        }
                    }
                } else if (!thisNode.getMyHash().equals(thisNode.getSuccessor().getMyHash())) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERY_STR_GLOBAL, thisNode.getSuccessor().getMyId(), initiator, result);
                    if(initiator.equals(getPortNumber())) {
                        while (!checkQuery) {

                        }
                    }
                } else {
                    globalQuery = result;
                }

                checkQuery = false;
                if(!globalQuery.isEmpty() && initiator.equals(getPortNumber())) {
                    globalQuery = globalQuery.trim().substring(0, globalQuery.length() - 1);
                    String[] globalValues = globalQuery.split("%");
                    Log.d(TAG, "Sending back the global result of length" + globalValues.length);

                    StringTokenizer tokenizer = new StringTokenizer(globalQuery, "%");
                    while (tokenizer.hasMoreTokens()) {
                        String[] values = new String[]{tokenizer.nextToken(), tokenizer.nextToken()};
                        cursor.addRow(values);
                    }
                }
                Log.d(TAG, "* : Number of rows returned is : " + cursor.getCount());
                return cursor;

            } else if( selection.matches("@")){
                for(String file : getContext().fileList()) {
                    fis = getContext().openFileInput(file);
                    reader = new BufferedReader(new InputStreamReader(fis));

                    while ((line = reader.readLine()) != null) {
                        value = line;
                    }
                    String[] values = new String[]{file, value.toString()};
                    cursor.addRow(values);
                }
                Log.d(TAG, "Number of rows returned is : " + cursor.getCount());
                return cursor;
            } else {
                hashQuery = genHash(selection);

                if( ring.size() == 1 || (hashQuery.compareTo(thisNode.getPredecessor().getMyHash()) > 0 && hashQuery.compareTo(thisNode.getMyHash()) <= 0) ||
                        (thisNode.getMyHash().equals(genHashPort(thisNode.getSmallest())) &&
                                ( hashQuery.compareTo(thisNode.getMyHash()) <= 0 || hashQuery.compareTo(genHashPort(thisNode.getLargest())) > 0 ))) {
                    Log.d(TAG, "Fetching the file from local");
                    fis = getContext().openFileInput(selection);
                    reader = new BufferedReader(new InputStreamReader(fis));

                    while ((line = reader.readLine()) != null) {
                        value = line;
                    }
                    Log.d(TAG, "Value returned returned is : " + value.toString());
                    Log.d(TAG, "Get port number : " + getPortNumber());
                    if( initiator.equals(getPortNumber()) ) {
                        String[] values = new String[]{selection, value.toString()};
                        cursor.addRow(values);
                        Log.d(TAG, "Number of rows returned is : " + cursor.getCount());
                        return cursor;
                    } else {
                        Log.d(TAG,"Result found : "+ value.toString() +" on "+getPortNumber());
                        Log.d(TAG,"Forwarding this result to "+ initiator);
                        new ClientTask().executeOnExecutor( AsyncTask.SERIAL_EXECUTOR, QUERY_STR_LOCAL, initiator, initiator, selection, FOUND_STR, value.toString() );
                    }
                }  else if( thisNode.getMyHash().equals( genHashPort(thisNode.getLargest()) )  && !thisNode.getMyHash().equals(genHashPort(thisNode.getSmallest()))) {
                    Log.d(TAG,"Forwarding the query to "+ thisNode.getSmallest());
                    new ClientTask().executeOnExecutor( AsyncTask.SERIAL_EXECUTOR, QUERY_STR_LOCAL, thisNode.getSmallest(), initiator, selection,"NotFound"," " );
                    if(initiator.equals(getPortNumber())) {
                        Log.e(TAG,"Condition for wait is true");
                        while (!checkQuery) {
                            //Log.e(TAG, "Waiting");
                        }
                    }
                }  else if (!thisNode.getMyHash().equals(thisNode.getSuccessor().getMyHash())) {
                    Log.d(TAG, "Forwarding the request to successor " + thisNode.getSuccessor().getMyId());
                    new ClientTask().executeOnExecutor( AsyncTask.SERIAL_EXECUTOR, QUERY_STR_LOCAL, thisNode.getSuccessor().getMyId(), initiator, selection,"NotFound"," " );
                    if(initiator.equals(getPortNumber())) {
                        Log.e(TAG,"Condition for wait is true");
                        while (!checkQuery) {
                            //Log.e(TAG, "Waiting");
                        }
                    }
                }

                Log.d(TAG,"Query : Block Over. The result is : "+valueFound);
                checkQuery = false;
                if(valueFound == null) {
                    return cursor;
                } else {
                    String[] values = new String[]{selection.trim(), valueFound.trim()};
                    cursor.addRow(values);
                    Log.d(TAG,"Result key : "+ selection);
                    Log.d(TAG,"Result value : "+ valueFound);
                    Log.d(TAG,"Query : Returning "+ cursor.getCount() + " values");
                    return cursor;
                }
            }
        } catch (Exception e) {
            Log.e(TAG, "Query Exception" + e.getMessage());
            e.printStackTrace();
        }
        return cursor;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String msgReceived = null;
            Socket socket;
            try {
                while(true) {
                    socket = serverSocket.accept();

                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
                    if ((msgReceived = reader.readLine()) != null) {

                        msgReceived = msgReceived.trim();
                        if(msgReceived.contains(JOIN_STR)){

                            String splitReceived[] = msgReceived.split( " " );
                            updateNodeInfo(splitReceived[1]);

                            Log.d(TAG, "ServerTask: Sending this join information to other "+ ring.size() +" nodes");

                            StringBuilder retVal = new StringBuilder();
                            if(ring.size() != 0) {
                                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                                for (String ringNode : ring) {
                                    retVal.append(ringNode);
                                    retVal.append(" ");
                                }
                                writer.println(retVal.toString().trim());
                            }
                        } else if(msgReceived.contains(UPDATE_STR)){
                            String splitReceived[] = msgReceived.split( " " );
                            Log.d(TAG, "ServerTask: Updating info for "+splitReceived[1]);
                            updateNodeInfo(splitReceived[1]);

                        } else if(msgReceived.contains(DELETE_STR)){
                            getContext().getContentResolver().delete(providerUri, "\"*\"", null);

                        } else if(msgReceived.contains(QUERY_STR_GLOBAL)){
                            String strReceiveSplit[] = msgReceived.split("@@");
                            //Log.d(TAG,"ServerTask: The cursor string received is : " + strReceiveSplit[2]);
                            if(strReceiveSplit[1].equals(getPortNumber())){
                                Log.d(TAG,"ServerTask : Toggle flag and set result");
                                if(strReceiveSplit.length > 2) {
                                    globalQuery = strReceiveSplit[2];
                                }
                                checkQuery = true;
                            } else {
                                String sendSelectArgs[];
                                if(strReceiveSplit.length > 2) {
                                    sendSelectArgs = new String[]{strReceiveSplit[1], strReceiveSplit[2]};
                                }else{
                                    sendSelectArgs = new String[]{strReceiveSplit[1], ""};
                                }
                                getContext().getContentResolver().query(providerUri, null, "*", sendSelectArgs, null);
                            }
                        } else if(msgReceived.contains(QUERY_STR_LOCAL)){
                            String strReceiveSplit[] = msgReceived.split("@@");
                            if( strReceiveSplit[3].equals( FOUND_STR ) ){
                                Log.d(TAG,"ServerTask : Toggle flag and set result");
                                valueFound = strReceiveSplit[4];
                                checkQuery = true;
                            } else if( strReceiveSplit[1].equals(getPortNumber()) ){
                                Log.d(TAG,"ServerTask : Result not found");
                                checkQuery = true;
                            } else {
                                String sendSelectArgs[] = new String[]{strReceiveSplit[1]};
                                getContext().getContentResolver().query(providerUri, null, strReceiveSplit[2], sendSelectArgs, null);
                            }
                        } else if(msgReceived.contains(INSERT_STR)){
                            msgReceived = msgReceived.replace(INSERT_STR,"");
                            ContentValues keyValueToInsert = new ContentValues();
                            StringTokenizer tokenizer = new StringTokenizer(msgReceived,"%%");
                            keyValueToInsert.put( "key", tokenizer.nextToken() );
                            keyValueToInsert.put( "value", tokenizer.nextToken() );

                            getContext().getContentResolver().insert( providerUri,  keyValueToInsert );

                        }

                    }
                }
            }catch (IOException ioe){
                Log.e(TAG, "ServerTask socket IOException");
            }catch(Exception e){
                Log.e(TAG, "ServerTask Exception Unknown"+ e.getMessage());
                e.printStackTrace();
                checkQuery = true;
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {

            String strReceived = strings[0].trim();
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String msgReceived = null;
            PrintWriter writer = null;
            Socket socket = null;
            Socket socket1 = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[1]));
                String msgToSend = msgs[0];

                if(msgToSend.equals(JOIN_STR)){
                    msgToSend = msgToSend + " " + msgs[2];
                    writer = new PrintWriter(socket.getOutputStream(), true);
                    Log.d(TAG, "ClientTask: Sending join request to 5554");
                    writer.println(msgToSend);

                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
                    String strNodes[];
                    if ((msgReceived = reader.readLine()) != null) {
                        Log.d(TAG, "ClientTask: Updating other nodes in the ring");
                        strNodes = msgReceived.split(" ");
                        Log.d(TAG,"ClientTask: The size of strNodes is : " + strNodes.length);
                        for( String node : strNodes ) {
                            if(node.equals(JOIN_PORT) || node.equals(msgs[2])){
                                continue;
                            }
                            socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(node));
                            Log.d(TAG, "ClientTask: Updating "+ msgs[2]+" in "+node);
                            writer = new PrintWriter(socket1.getOutputStream(), true);
                            msgToSend = UPDATE_STR + " " + msgs[2];
                            writer.println( msgToSend );
                        }
                        for( String node : strNodes ){
                            updateNodeInfo(node);
                        }
                    }
                } else if( msgToSend.equals( DELETE_STR ) ) {
                    writer = new PrintWriter( socket.getOutputStream(), true );
                    writer.println(msgToSend);
                } else if( msgToSend.equals( QUERY_STR_GLOBAL ) ) {

                    writer = new PrintWriter( socket.getOutputStream(), true );
                    writer.println( msgToSend + "@@" + msgs[2] + "@@" + msgs[3] );

                } else if( msgToSend.equals( QUERY_STR_LOCAL ) ) {

                    writer = new PrintWriter( socket.getOutputStream(), true );
                    writer.println( msgToSend + "@@" + msgs[2] + "@@" + msgs[3] + "@@" +msgs[4] + "@@" + msgs[5] );

                } else if( msgToSend.equals( INSERT_STR ) ) {
                    writer = new PrintWriter( socket.getOutputStream(), true );
                    writer.println( INSERT_STR + msgs[2]);
                }
                if(socket1 != null) {
                    socket1.close();
                }
                if(socket != null) {
                    socket.close();
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException"+e.getMessage());
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException"+e.getMessage());
            }

            return null;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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

    private String genHashPort(String input) throws NoSuchAlgorithmException {
        input = String.valueOf(Integer.parseInt(input)/2);
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String getPortNumber() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return String.valueOf((Integer.parseInt(portStr) * 2));
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private void prepareThisNode(String myId, String predecessor, String successor){

        Log.d(TAG, "Preparing node");
        Node pred = new Node();
        Node succ = new Node();

        try {
            thisNode.setMyId( myId );
            thisNode.setMyHash( genHashPort(myId) );

            pred.setMyId( myId );
            pred.setMyHash( genHashPort(myId) );

            thisNode.setPredecessor( pred );

            succ.setMyId( myId );
            succ.setMyHash( genHashPort(myId) );

            thisNode.setSuccessor(succ);
            thisNode.setSmallest( myId );
            thisNode.setLargest( myId );
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private void updateNodeInfo(String nodeId ){

        Log.d(TAG, "Updating node successor predecessor information");
        Log.d(TAG,"New Node is "+ nodeId);
        Log.d(TAG,"This Node is "+ thisNode.getMyId());

        try {
            String hashNode = genHashPort( nodeId );

            if( hashNode.compareTo(thisNode.getMyHash()) < 0 ){
                if((thisNode.getMyHash().equals(thisNode.getPredecessor().getMyHash()) && hashNode.compareTo(thisNode.getPredecessor().getMyHash()) < 0) ||
                        hashNode.compareTo(thisNode.getPredecessor().getMyHash()) > 0) {
                    thisNode.setPredecessor(new Node());
                    thisNode.getPredecessor().setMyId(nodeId);
                    thisNode.getPredecessor().setMyHash(hashNode);
                }
            } else if( hashNode.compareTo(thisNode.getMyHash()) > 0){
                if((thisNode.getMyHash().equals(thisNode.getSuccessor().getMyHash()) && hashNode.compareTo(thisNode.getSuccessor().getMyHash()) > 0) ||
                        hashNode.compareTo(thisNode.getSuccessor().getMyHash()) < 0) {
                    thisNode.setSuccessor(new Node());
                    thisNode.getSuccessor().setMyId(nodeId);
                    thisNode.getSuccessor().setMyHash(hashNode);
                }
            }
            if(hashNode.compareTo( genHashPort( thisNode.getSmallest() ) ) < 0){
                thisNode.setSmallest(nodeId);
            }
            if(hashNode.compareTo( genHashPort( thisNode.getLargest() ) ) > 0){
                thisNode.setLargest(nodeId);
            }
            ring.add(nodeId);
            Log.d(TAG, "UpdateNodeInfo: The size of the ring is "+ring.size());
            Log.d(TAG,"Predecessor Node is "+ thisNode.getPredecessor().getMyId());
            Log.d(TAG,"Successor Node is "+ thisNode.getSuccessor().getMyId());
            Log.d(TAG,"the largest node is : "+thisNode.getLargest());
            Log.d(TAG,"the smallest node is : "+thisNode.getSmallest());
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG,"UpdateNodeInfo: Exception");
        }
    }

}


