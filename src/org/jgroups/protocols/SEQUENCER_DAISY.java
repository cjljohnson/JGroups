
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;


/**
 * Implementation of a sequencer designed for use with DAISYCHAIN protocol.
 * Message is broadcast by the sender, then sequence number broadcast separately by coordinator.
 * Messages are buffered at receivers until sequence number is received from coordinator.
 * @author Chris Johnson
 */
@MBean(description="Implementation of total order protocol using a sequencer")
public class SEQUENCER_DAISY extends Protocol {
    protected Address                           local_addr;
    protected volatile Address                  coord;
    protected volatile View                     view;
    @ManagedAttribute
    protected volatile boolean                  is_coord;
    protected final AtomicLong                  seqno=new AtomicLong(0);

    protected final AtomicLong                  order_id=new AtomicLong(0);
    protected final AtomicLong                  next_id=new AtomicLong(1);


    /** Maintains messages forwarded to the coord which which no ack has been received yet.
     *  Needs to be sorted so we resend them in the right order
     */
    protected final NavigableMap<Long,Message>  forward_table=new ConcurrentSkipListMap<>();

    protected final Lock                        send_lock=new ReentrantLock();

    protected final Condition                   send_cond=send_lock.newCondition();

    /** When ack_mode is set, we need to wait for an ack for each forwarded message until we can send the next one */
    @ManagedAttribute(description="is ack-mode enabled or not")
    protected volatile boolean                  ack_mode=true;

    /** Set when we block all sending threads to resend all messages from forward_table */
    protected volatile boolean                  flushing;

    protected volatile boolean                  running=true;

    /** Keeps track of the threads sending messages */
    protected final AtomicInteger               in_flight_sends=new AtomicInteger(0);

    // Maintains received seqnos, so we can weed out dupes
    protected final ConcurrentMap<Address,BoundedHashMap<Long,Long>> delivery_table=Util.createConcurrentMap();


    /** Used for each resent message to wait until the message has been received */
    protected final Promise<Long>               ack_promise=new Promise<>();

    protected MessageFactory                    msg_factory;



    @Property(description="Size of the set to store received seqnos (for duplicate checking)")
    protected int                               delivery_table_max_size=2000;

    @Property(description="Number of acks needed before going from ack-mode to normal mode. " +
      "0 disables this, which means that ack-mode is always on")
    protected int                               threshold=10;

    @Property(description="If true, all messages in the forward-table are sent to the new coord, else thye're " +
      "dropped (https://issues.jboss.org/browse/JGRP-2268)")
    protected boolean                           flush_forward_table=true;


    protected final Map<Address, Queue<Message>>   broadcast_queues=new ConcurrentHashMap<>();
    protected final PriorityBlockingQueue<MessageOrder>    order_queue=new PriorityBlockingQueue<>();

    @ManagedAttribute(description="tracing is enabled or disabled for the given log",writable=true)
    protected boolean is_trace=log.isTraceEnabled();

    @ManagedAttribute protected int  num_acks;
    @ManagedAttribute protected long forwarded_msgs;
    @ManagedAttribute protected long bcast_msgs;
    @ManagedAttribute protected long received_forwards;
    @ManagedAttribute protected long received_bcasts;
    @ManagedAttribute protected long delivered_bcasts;

    @ManagedAttribute
    public boolean isCoordinator() {return is_coord;}
    public Address getCoordinator() {return coord;}
    public Address getLocalAddress() {return local_addr;}

    @ManagedAttribute(description="Number of messages in the forward-table")
    public int getForwardTableSize() {return forward_table.size();}

    public SEQUENCER_DAISY setThreshold(int new_threshold) {this.threshold=new_threshold; return this;}

    public boolean print = false;

    @ManagedOperation
    public void resetStats() {
        forwarded_msgs=bcast_msgs=received_forwards=received_bcasts=delivered_bcasts=0L;
    }

    public void init() throws Exception {
        super.init();
        msg_factory=getTransport().getMessageFactory();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleViewChange(evt.getArg());
                break;

            case Event.TMP_VIEW:
                handleTmpView(evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object down(Message msg) {
        if(msg.getDest() != null || msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
            return down_prot.down(msg);

        if(msg.getSrc() == null)
            msg.setSrc(local_addr);

        // A seqno is not used to establish ordering, but only to weed out duplicates; next_seqno doesn't need
        // to increase monotonically, but only to be unique (https://issues.jboss.org/browse/JGRP-1461) !
        long next_seqno=seqno.incrementAndGet();

        SequencerHeader hdr=new SequencerHeader(SequencerHeader.BCAST, next_seqno);
        msg.putHeader(this.id, hdr);
        if(log.isTraceEnabled())
            log.trace("[" + local_addr + "]: broadcasting " + local_addr + "::" + seqno);

        return down_prot.down(msg);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                Object retval=up_prot.up(evt);
                handleViewChange(evt.getArg());
                return retval;

            case Event.TMP_VIEW:
                handleTmpView(evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        Header hdr;
        if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
            return up_prot.up(msg);
        hdr=msg.getHeader(this.id);
        if(hdr == null)
            return up_prot.up(msg); // pass up

        if(hdr instanceof MessageOrder)
            receiveOrderID(msg, (MessageOrder)hdr);

        if(hdr instanceof SequencerHeader)
            receiveBroadcast(msg, (SequencerHeader)hdr);

        return null;
    }

    public void up(MessageBatch batch) {
        MessageIterator it=batch.iterator();
        while(it.hasNext()) {
            Message msg=it.next();
            if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB) || msg.getHeader(id) == null)
                continue;
            it.remove();

            // simplistic implementation
            try {
                up(msg);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedPassingUpMessage"), t);
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    /* --------------------------------- Private Methods ----------------------------------- */

    protected void receiveBroadcast(Message msg, SequencerHeader hdr) {
        if (hdr.type != SequencerHeader.BCAST)
            return;

        Queue<Message> src_queue=broadcast_queues.get(msg.getSrc());
        if(src_queue == null) {
            src_queue=new LinkedBlockingQueue<>();
            broadcast_queues.put(msg.getSrc(), src_queue);
        }
        src_queue.add(msg);

        if (is_coord) {
            if (print)
            System.out.println("Create order for: " + hdr.seqno);
            long msg_id = order_id.incrementAndGet();
            Message order_msg=new EmptyMessage(null)
                    .setSrc(local_addr)
                    .putHeader(id, new MessageOrder(msg.getSrc(), hdr.seqno, msg_id))
                    .setFlag(Message.Flag.INTERNAL);
            down_prot.down(order_msg);
        }
    }

    protected void receiveOrderID(Message msg, MessageOrder hdr) {
        if (print)
            System.out.println("ID: " + hdr.order_id);

        order_queue.add(hdr);
    }

    long last_delivered = 0;
    boolean was_blocked = false;

    protected synchronized void tryPassup() {
        int deliveries = 0;
        while (true) {
            if (print)
            System.out.println("TRY PASSUP");
            MessageOrder msg_order = order_queue.peek();
            if (msg_order != null && print)
                System.out.println("Order comparison: " + msg_order.order_id + ", " + next_id.get());
            if (msg_order == null || msg_order.order_id != next_id.get())
                break;
            if (print)
            System.out.println("NEXT ORDER: " + msg_order.order_id + ", " + next_id.get());
            Queue<Message> sender_queue = broadcast_queues.get(msg_order.msg_src);
            if(sender_queue == null)
                break;
            if (print)
            System.out.println("Q: " + msg_order.msg_src);
            Message msg = sender_queue.peek();
            if (msg == null)
                break;
            if (print)
            System.out.println("msg: " + msg);
            SequencerHeader hdr=msg.getHeader(this.id);
            if(hdr == null)
                break;
//            if (hdr.getSeqno() < msg_order.seqno) {
//                System.out.println("LOWER SEQ: " + msg_order.seqno + ", " + hdr.seqno);
//                //sender_queue.poll();
//                continue;
//            }
//            if (hdr.getSeqno() > msg_order.seqno) {
//                System.out.println("HIGHER SEQ: " + msg_order.seqno + ", " + hdr.seqno);
//                //sender_queue.poll();
//                continue;
//            }
            deliver(msg, msg.getSrc(), msg_order.order_id, "SEQUENCER_DAISY");
            last_delivered = msg_order.order_id;
            deliveries++;

            if (was_blocked) {
                was_blocked = false;
                System.out.println("DELIVERED: " + msg_order.order_id);
            }

            //System.out.println("delivered: " + msg_order.order_id + ", " + msg_order.msg_src + ", " + msg_order.seqno);
            order_queue.poll();
            sender_queue.poll();
            next_id.incrementAndGet();
        }
//        if (deliveries == 0) {
//            was_blocked = true;
//            MessageOrder msg_order = order_queue.peek();
//            if(msg_order == null)
//                return;
//
//            if (msg_order != null)
//                System.out.println("STATE: " + next_id.get() + ", " + msg_order.order_id + ", " + msg_order.seqno  + ", " + msg_order.msg_src + ", " + last_delivered);
//            Queue<Message> sender_queue = broadcast_queues.get(msg_order.msg_src);
//            if(sender_queue == null)
//                return;
//            Message msg = sender_queue.peek();
//            if (msg == null)
//                return;
//            SequencerHeader hdr=msg.getHeader(this.id);
//            System.out.println("Q STATE: " + hdr.seqno);
//        }

    }

    protected void deliver(Message msg, Address sender, long seqno, String error_msg) {
        if(is_trace)
            log.trace("%s <-- %s: #%d", local_addr, sender, seqno);
        try {
            up_prot.up(msg);
        }
        catch(Throwable t) {
            log.error(Util.getMessage("FailedToDeliverMsg"), local_addr, error_msg, msg, t);
        }
    }

    protected void handleViewChange(View v) {
        List<Address> mbrs=v.getMembers();
        if(mbrs.isEmpty()) return;

        if(view == null || view.compareTo(v) < 0)
            view=v;
        else
            return;

        delivery_table.keySet().retainAll(mbrs);



        Address existing_coord=coord, new_coord=mbrs.get(0);
        boolean coord_changed=!Objects.equals(existing_coord, new_coord);
        if(coord_changed && new_coord != null) {

            coord=new_coord;
            is_coord=Objects.equals(local_addr, coord);
        }
    }

    protected TimeScheduler timer=null;
    protected Future<?> passup_task;
    protected long    passup_interval=1;
    protected boolean passup_can_block=true;

    public void start() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer is null");
        running=true;
        startPassupTask();
    }

    public void stop() {
        running=false;
        stopFlushTask();
    }

    protected void startPassupTask() {
        if(passup_task == null || passup_task.isDone())
            passup_task=timer.scheduleWithFixedDelay(new SEQUENCER_DAISY.PassupTask(), 0, passup_interval, TimeUnit.MILLISECONDS, passup_can_block);
    }

    protected void stopFlushTask() {
        if(passup_task != null) {
            passup_task.cancel(true);
            passup_task=null;
        }
    }

    protected class PassupTask implements Runnable {
        public void run() {
            passup();
        }

        public String toString() {
            return EARLYBATCH.class.getSimpleName() + ": FlushTask (interval=" + passup_interval + " ms)";
        }
    }

    public void passup() {
        tryPassup();
    }

    // If we're becoming coordinator, we need to handle TMP_VIEW as
    // an immediate change of view. See JGRP-1452.
    private void handleTmpView(View v) {
        Address new_coord=v.getCoord();
        if(new_coord != null && !new_coord.equals(coord) && local_addr != null && local_addr.equals(new_coord))
            handleViewChange(v);
    }

/* ----------------------------- End of Private Methods -------------------------------- */

    public static class MessageOrder extends Header implements Comparable<MessageOrder> {
        protected Address msg_src;
        protected long seqno;
        protected long order_id;

        public MessageOrder(Address msg_src, long seqno, long order_id) {
            this.msg_src=msg_src;
            this.seqno=seqno;
            this.order_id=order_id;
        }

        public MessageOrder() {
        }

        @Override
        public int compareTo(MessageOrder o) {
            if (this.order_id == o.order_id)
                return 0;
            return this.order_id > o.order_id ? 1 : -1;
        }

        @Override
        public Supplier<? extends Header> create() {
            return MessageOrder::new;
        }

        @Override
        public short getMagicId() {
            return 97;
        }

        @Override
        public int serializedSize() {
            return Util.size(msg_src) + Bits.size(seqno) + Bits.size(order_id);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            if(msg_src != null)
                Util.writeAddress(msg_src, out);
            Bits.writeLongCompressed(seqno, out);
            Bits.writeLongCompressed(order_id, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            msg_src=Util.readAddress(in);
            seqno=Bits.readLongCompressed(in);
            order_id=Bits.readLongCompressed(in);
        }
    }


    public static class SequencerHeader extends Header {
        protected static final byte BCAST       = 1;

        protected byte    type=-1;
        protected long    seqno=-1;
        protected boolean flush_ack;

        //protected Message next;
        //protected Message previous;

        public SequencerHeader() {
        }

        public SequencerHeader(byte type) {
            this.type=type;
        }

        public SequencerHeader(byte type, long seqno) {
            this(type);
            this.seqno=seqno;
        }


        public short getMagicId() {return 96;}

        public long getSeqno() {
            return seqno;
        }

        public Supplier<? extends Header> create() {return SequencerHeader::new;}

        public String toString() {
            StringBuilder sb=new StringBuilder(64);
            sb.append(printType());
            if(seqno >= 0)
                sb.append(" seqno=" + seqno);
            if(flush_ack)
                sb.append(" (flush_ack)");
            return sb.toString();
        }

        protected final String printType() {
            switch(type) {
                case BCAST:          return "BCAST";
                default:             return "n/a";
            }
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeByte(type);
            Bits.writeLongCompressed(seqno, out);
            out.writeBoolean(flush_ack);
            // add new stuff
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            type=in.readByte();
            seqno=Bits.readLongCompressed(in);
            flush_ack=in.readBoolean();
            // add new stuff
        }

        @Override
        public int serializedSize() {
            return Global.BYTE_SIZE + Bits.size(seqno) + Global.BYTE_SIZE; // type + seqno + flush_ack
        }

    }

}
