package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AckChecker;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Batches messages near the top of the stack.  This reduces the work done on the IO thread and reduces overhead,
 * greatly increasing throughput for smaller message sizes (<1k in test configurations).  Also reduces the amount of
 * header data by having one header for each batch.
 * Conceptually, down messages are buffered then put in a wrapper message, so lower protocols only interact with the
 * wrapper.  On the receiving end, the batch is unwrapped when it reaches this protocol and then forwarded to higher
 * protocols as individual messages in a loop.
 * @author Chris Johnson
 * @since 5.x
 */
@Experimental
@MBean(description="Protocol just below flow control that wraps messages to improve throughput with small messages.")
public class EARLYBATCH extends Protocol {

    @ManagedAttribute(description="Local address")
    protected volatile Address       local_addr;

    @ManagedAttribute(description="The current view")
    protected volatile View          view;

    @ManagedAttribute(type=AttributeType.SCALAR)
    protected int                    msgs_sent;

    public EarlyBatchHeader header = new EarlyBatchHeader();

    public static final int MAXBATCHSIZE = 100;
    // EOFException if >60k
    public static final int MAXBATCHBYTES = 50000;
    protected ConcurrentMap<Address, EarlyBatchBuffer> msgMap = Util.createConcurrentMap();

    protected final ReentrantLock lock=new ReentrantLock();
    protected final NullAddress nullAddress = new NullAddress();

    protected TimeScheduler timer=null;
    protected volatile boolean running=false;
    protected Future<?> flush_task;
    protected long    flush_interval=100;
    protected boolean flush_can_block=true;


    public void init() throws Exception {
        msgMap.putIfAbsent(nullAddress, new EarlyBatchBuffer(nullAddress, this, MAXBATCHBYTES));
    }

    public void resetStats() {
        super.resetStats();
        msgs_sent=0;
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.CONFIG:
                //handleConfigEvent(evt.getArg());
                break;

            case Event.VIEW_CHANGE:
                handleViewChange(((View)evt.getArg()).getMembers());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt); // this could potentially use the lower protocol's thread which may block
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleViewChange(((View)evt.getArg()).getMembers());
                break;
        }
        return up_prot.up(evt);
    }

    protected void handleViewChange(List<Address> mbrs) {
        if(mbrs == null) return;

        mbrs.stream().filter(dest -> !msgMap.containsKey(dest)).forEach(dest -> msgMap.putIfAbsent(dest, new EarlyBatchBuffer(dest, this, MAXBATCHBYTES)));

        // remove members that left
        //msgMap.keySet().retainAll(mbrs);
        // Tries to send remaining messages so could potentially block, might not be necessary?
        // Potentially can forward messages out of order as remove and close are not synced but it isn't in view anyway
        msgMap.keySet().stream().filter(dest -> !mbrs.contains(dest) && !(dest instanceof NullAddress)).forEach(dest -> {
            EarlyBatchBuffer removed = msgMap.remove(dest);
            removed.close();
        });
    }

    public Object down(Message msg) {

        if (msg.isFlagSet(Message.Flag.OOB) || msg.isFlagSet(Message.Flag.INTERNAL)) {
            return down_prot.down(msg);
        }

        if (msg.getSrc() == null) {
            msg.setSrc(local_addr);
        }

        // Ignore messages from other senders due to EarlyBatchMessage compression
        if (!Objects.equals(msg.getSrc(), local_addr)) {
            return down_prot.down(msg);
        }

        //lock.lock();
        try {
            Address dest = msg.dest() == null ? nullAddress : msg.dest();
            EarlyBatchBuffer ebbuffer = msgMap.get(dest);
            if (ebbuffer == null) {
                return down_prot.down(msg);
            }
            boolean add_successful = ebbuffer.addMessage(msg);

            if (!add_successful) {
                return down_prot.down(msg);
            }

            //System.out.println("Added message to " + dest);
        } finally {
            //lock.unlock();
        }
        return msg;
    }

    public Object up(Message msg) {
        if(msg.getHeader(getId()) == null)
            return up_prot.up(msg);

        EarlyBatchMessage comp = (EarlyBatchMessage) msg;

        for(Iterator<Message> it = comp.iterator(); it.hasNext();) {
            final Message bundledMsg=it.next();
            bundledMsg.setDest(comp.getDest());
            if (bundledMsg.getSrc() == null)
                bundledMsg.setSrc(comp.getSrc());
            up_prot.up(bundledMsg);
        }
        return msg;
    }

    public void up(MessageBatch batch) {
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
                try {
                    up(msg);
                }
                catch(Throwable t) {
                    //log.error(Util.getMessage("PassUpFailure"), t);
                    t.printStackTrace();
                }
        }
    }

    public void start() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer is null");
        running=true;
        //leaving=false;
        startFlushTask();
    }

    public void stop() {
        running=false;
        //is_server=false;
        //if(become_server_queue != null)
        //    become_server_queue.clear();
        stopFlushTask();
        //xmit_task_map.clear();
        //reset();
    }

    protected void startFlushTask() {
        if(flush_task == null || flush_task.isDone())
            flush_task=timer.scheduleWithFixedDelay(new EARLYBATCH.FlushTask(), 0, flush_interval, TimeUnit.MILLISECONDS, flush_can_block);
    }

    protected void stopFlushTask() {
        if(flush_task != null) {
            flush_task.cancel(true);
            flush_task=null;
        }
    }

    protected class FlushTask implements Runnable {
        public void run() {
            flush();
        }

        public String toString() {
            return EARLYBATCH.class.getSimpleName() + ": FlushTask (interval=" + flush_interval + " ms)";
        }
    }

    public void flush() {
        msgMap.forEach((k,v) -> v.sendBatch());
        //System.out.println("flush");
    }

    protected static class EarlyBatchBuffer {

        private Address dest;
        private Message[] msgs;
        private int index;
        private EARLYBATCH ebprot;
        private boolean closed;
        private long total_bytes;
        private final long max_bytes;

        protected EarlyBatchBuffer(Address address, EARLYBATCH ebprot, long max_bytes) {
            this.dest=address;
            this.msgs = new Message[EARLYBATCH.MAXBATCHSIZE];
            this.index = 0;
            this.ebprot = ebprot;
            this.max_bytes = max_bytes;
        }

        protected synchronized boolean addMessage(Message msg) {
            if (closed) {
                return false;
            }

            int msg_bytes = msg.size();
            if((max_bytes > 0 && total_bytes + msg_bytes > max_bytes) ||
                    total_bytes + msg_bytes > ebprot.getTransport().getMaxBundleSize()) {
                sendBatch();
            }

            msgs[index++] = msg;
            total_bytes += msg_bytes;
            if (index == msgs.length) {
                sendBatch();
            }
            return true;
        }

        protected synchronized void sendBatch() {

            if (index == 0) {
                return;
            }

            if (index == 1) {
                ebprot.getDownProtocol().down(msgs[0]);
                msgs[0] = null;
                index = 0;
                total_bytes = 0;
                return;
            }

            Address ebdest = dest instanceof NullAddress ? null : dest;

            EarlyBatchMessage comp = new EarlyBatchMessage(ebdest, ebprot.local_addr, msgs, index);
            comp.putHeader(ebprot.getId(), ebprot.header);
            comp.setSrc(ebprot.local_addr);
            msgs = new Message[EARLYBATCH.MAXBATCHSIZE];
            index = 0;
            total_bytes = 0;
            // Could send down out of synchronize, but that could make batches hit nakack out of order
            ebprot.getDownProtocol().down(comp);
        }

        protected synchronized void close() {
            this.closed = true;
            sendBatch();
        }
    }

    public static class EarlyBatchHeader extends Header {

        public EarlyBatchHeader() {
        }

        public short getMagicId()      {return 95;}

        public Supplier<? extends Header> create() {return EarlyBatchHeader::new;}

        @Override public int  serializedSize()                           {return 0;}

        @Override public void writeTo(DataOutput out) throws IOException {
        }
        @Override public void readFrom(DataInput in) throws IOException  {
        }
        public String         toString()                                 {return "EarlyBatchHeader";}
    }

}
