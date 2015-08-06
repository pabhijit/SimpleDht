package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class SimpleDhtActivity extends Activity {

    private static final String TAG = SimpleDhtActivity.class.getSimpleName();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        findViewById(R.id.button1).setOnClickListener(
                new View.OnClickListener(){
                    @Override
                    public void onClick(View view) {
                           // new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "\"@\"","11108");
                        }
                });

        findViewById(R.id.button2).setOnClickListener(
                new View.OnClickListener(){
                    @Override
                    public void onClick(View view) {
                       // new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "\"*\"","11108");
                    }
                });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
