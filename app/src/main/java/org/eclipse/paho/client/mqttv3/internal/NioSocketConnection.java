package org.eclipse.paho.client.mqttv3.internal;


import android.util.Log;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttToken;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttAck;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttDisconnect;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttPubAck;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttPubComp;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttPubRec;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttWireMessage;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class NioSocketConnection implements Runnable {
    private static final String TAG = "NioSocketConnection";

    private SocketAddress mAddress;
    private SocketChannel mSocketChannel;
    private Selector mSelector;
    private boolean mIsRunning = false;

    private ClientComms mClientComms;
    private ClientState mClientState;

    private CommsTokenStore mTokenStore;

    private ExecutorService mExecutorService;

    private Future mFuture;

    public NioSocketConnection(ClientComms clientComms, SocketAddress address,
                               ClientState clientState, CommsTokenStore tokenStore) {
        mClientComms = clientComms;
        mAddress = address;
        mClientState = clientState;
        mTokenStore = tokenStore;
    }

    // connect broker
    public void connect(ExecutorService executorService) {
        initConnection();
        mExecutorService = executorService;
        mFuture = mExecutorService.submit(this);
    }

    private void initConnection() {
        try {

            if (mSocketChannel != null && mSocketChannel.isConnectionPending()) {
                return;
            }
            if (mSocketChannel != null && mSocketChannel.isOpen()) {
                mSocketChannel.close();
            }
            mSelector = Selector.open();
            mSocketChannel = SocketChannel.open();
            mSocketChannel.configureBlocking(false);
            mSocketChannel.connect(mAddress);
            mSocketChannel.register(mSelector, SelectionKey.OP_CONNECT);
        } catch (Exception e) {
            mClientComms.shutdownConnection(null, new MqttException(e));
            e.printStackTrace();
        }
    }

    // stop the connection
    public void stop() {
        try {
            if (mFuture != null) {
                mFuture.cancel(true);
            }
        } catch (Exception e) {

        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                mSelector.select();
                Iterator<SelectionKey> iterator = mSelector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();
                    handleSelectionKey(selectionKey);
                    iterator.remove();
                }
            }
        } catch (IOException e) {

            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
    private void handleSelectionKey(SelectionKey key) throws InterruptedException {
        if (key == null) {
            return;
        }
        if (key.isValid()) {
            if (key.isConnectable()) {
                try {
                    SocketChannel channel = (SocketChannel) key.channel();
                    if (channel.finishConnect()) {
                        key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    handleRunException(null, e);
                }
            }

            if (key.isReadable()) {
                try {
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    SocketChannel channel = (SocketChannel) key.channel();
                    channel.read(buffer);

                    handleRead(buffer.array());
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    System.out.println("read ex:" + e.toString());
                }
            }
            if (key.isWritable()) {
                try {
                    final SocketChannel channel = (SocketChannel) key.channel();
                    mExecutorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                // mClientState.get()会阻塞线程的执行，故发送消息需要在新线程内完成
                                handleWrite(channel);
                            } catch (IOException e) {
                                e.printStackTrace();
                                handleRunException(null, e);
                            }
                        }
                    });
                } catch (Exception e) {
                    System.out.println("write ex:" + e.toString());
                }
            }
        }
    }

    private void handleWrite(SocketChannel channel) throws IOException {
        MqttWireMessage message = null;
        try {
            message = mClientState.get();
            if (message != null) {
                //@TRACE 802=network send key={0} msg={1}
                byte [] data = message.message2bytes();
                if (message instanceof MqttAck) {
                    channel.write(ByteBuffer.wrap(data));
                } else {
                    MqttToken token = mTokenStore.getToken(message);
                    // While quiescing the tokenstore can be cleared so need
                    // to check for null for the case where clear occurs
                    // while trying to send a message.
                    if (token != null) {
                        synchronized (token) {
                            try {
                                channel.write(ByteBuffer.wrap(data));
                                mClientState.notifySentBytes(data.length);
                            } catch (IOException ex) {
                                System.out.println("ex connect status:" + mSocketChannel.isConnected());
                                // The flush has been seen to fail on disconnect of a SSL socket
                                // as disconnect is in progress this should not be treated as an error
                                if (!(message instanceof MqttDisconnect)) {
                                    throw ex;
                                }
                            }
                            mClientState.notifySent(message);
                        }
                    }
                }
            } else { // null message
                //@TRACE 803=get message returned null, stopping}
                mIsRunning = false;
            }
        } catch (MqttException e) {
            e.printStackTrace();
            handleRunException(message, e);
        } catch (Exception ex) {
            handleRunException(message, ex);
        }

    }


    private void handleRead(byte [] data) {
        MqttToken token = null;
        try {
            //@TRACE 852=network read message
            mIsRunning = true;
            MqttWireMessage message = MqttWireMessage.createWireMessage(data);
            // instanceof checks if message is null
            if (message instanceof MqttAck) {
                token = mTokenStore.getToken(message);
                if (token!=null) {
                    synchronized (token) {
                        // Ensure the notify processing is done under a lock on the token
                        // This ensures that the send processing can complete  before the
                        // receive processing starts! ( request and ack and ack processing
                        // can occur before request processing is complete if not!
                        mClientState.notifyReceivedAck((MqttAck)message);
                    }
                } else if(message instanceof MqttPubRec || message instanceof MqttPubComp || message instanceof MqttPubAck) {
                    //This is an ack for a message we no longer have a ticket for.
                    //This probably means we already received this message and it's being send again
                    //because of timeouts, crashes, disconnects, restarts etc.
                    //It should be safe to ignore these unexpected messages.
                } else {
                    // It its an ack and there is no token then something is not right.
                    // An ack should always have a token assoicated with it.
                    throw new MqttException(MqttException.REASON_CODE_UNEXPECTED_ERROR);
                }
            } else {
                if (message != null) {
                    // A new message has arrived
                    mClientState.notifyReceivedMsg(message);
                }
            }
        } catch (MqttException ex) {
            //@TRACE 856=Stopping, MQttException
            mIsRunning = false;
            // Token maybe null but that is handled in shutdown
            mClientComms.shutdownConnection(token, ex);
        } finally {
            mIsRunning = false;
        }
    }

    private void handleRunException(MqttWireMessage message, Exception ex) {
        final String methodName = "handleRunException";
        //@TRACE 804=exception
        MqttException mex;
        if ( !(ex instanceof MqttException)) {
            mex = new MqttException(MqttException.REASON_CODE_CONNECTION_LOST, ex);
        } else {
            mex = (MqttException)ex;
        }

        mIsRunning = false;
        mClientComms.shutdownConnection(null, mex);
    }
}
