package com.rock.mqttdemoproj;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.text.TextUtils;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import org.eclipse.paho.android.service.MqttAndroidClient;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.w3c.dom.Text;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import butterknife.BindView;
import butterknife.ButterKnife;
import butterknife.OnClick;

public class MainActivity extends AppCompatActivity {

    @BindView(R.id.log_pannel) TextView mLogContainer;
    @BindView(R.id.subscribe_topic) EditText mSubscribeTopicEdt;
    @BindView(R.id.publish_topic) EditText mPublishTopicEdt;
    @BindView(R.id.publish_content) EditText mPublishContentEdt;
    private final String BROKER = "tcp://118.24.153.230:61613";
    private final String USERNAME = "admin";
    private final String PASSWORD = "password";
    private String mClientId;
    private MqttAndroidClient mClient;

    private SimpleDateFormat mSimpleDateFormat = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        ButterKnife.bind(this);
    }

    @OnClick(R.id.connect)
    public void connect(View view) {
        mClientId = "rock" + UUID.randomUUID();
        try {
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(false);
            connOpts.setUserName(USERNAME);
            connOpts.setPassword(PASSWORD.toCharArray());
            connOpts.setConnectionTimeout(10);
            connOpts.setKeepAliveInterval(20);
            connOpts.setAutomaticReconnect(true);
            mClient = new MqttAndroidClient(this, BROKER, mClientId);
            mClient.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    showLog("connect complete:" + serverURI);
                }

                @Override
                public void connectionLost(Throwable cause) {
                    String msg = "connection lost";
                    if (cause != null) {
                        msg += cause.getMessage();
                    }
                    showLog(msg);
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    showLog("messageArrived:" + topic + "-" + new String(message.getPayload()));
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    showLog("message deliveryComplete");
                }
            });
            IMqttToken token = mClient.connect(connOpts);
            token.setActionCallback(new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    showLog("connect success");
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    showLog("connect fail:" + exception.getMessage());
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @OnClick(R.id.disconnect)
    public void disconnect(View view) {
        if (mClient != null && mClient.isConnected()) {
            try {
                mClient.disconnect();
            } catch (Exception e) {

            }

        }
    }

    private void showLog(String log) {
        if (!TextUtils.isEmpty(log)) {
            String origin = mLogContainer.getText().toString();
            origin += "\n";
            String content = mSimpleDateFormat.format(new Date()) + ":" + log;
            origin += content;
            mLogContainer.setText(origin);
        }
    }

    @OnClick(R.id.subscribe)
    public void subscribe(View view) {
        final String topic = mSubscribeTopicEdt.getText().toString();
        if (!TextUtils.isEmpty(topic)) {
            try {
                IMqttToken token = mClient.subscribe(topic, 1);
                token.setActionCallback(new IMqttActionListener() {
                    @Override
                    public void onSuccess(IMqttToken asyncActionToken) {
                        showLog("subscrie success:" + topic);
                    }

                    @Override
                    public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                        String msg = "suscribe fail";
                        if (exception != null) {
                            msg += exception.getMessage();
                        }
                        showLog(msg);
                    }
                });
            } catch (Exception e) {

            }

        }
    }

    @OnClick(R.id.publish)
    public void publish(View view) {
        final String topic = mPublishTopicEdt.getText().toString();
        final String msg = mPublishContentEdt.getText().toString();
        if (!TextUtils.isEmpty(topic) && !TextUtils.isEmpty(msg)) {
            try {
                MqttMessage mqttMsg = new MqttMessage();
                mqttMsg.setPayload(msg.getBytes());
                mqttMsg.setQos(0);
                IMqttToken token = mClient.publish(topic, mqttMsg);
                token.setActionCallback(new IMqttActionListener() {
                    @Override
                    public void onSuccess(IMqttToken asyncActionToken) {
                        showLog("publish success:" + topic + "-" + msg);
                    }

                    @Override
                    public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                        showLog("publish fail:" + topic + "-" + msg);
                    }
                });
            } catch (Exception e) {

            }
        }
    }
}
