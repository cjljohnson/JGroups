package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;

import java.util.*;
import java.util.function.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Represents a message batch; multiple messages from the same sender to the same receiver(s). This class is unsynchronized.
 * @author Bela Ban
 * @since  3.3
 */
public class MessageBatch implements Iterable<Message> {

    /** The destination address. Null if this is a multicast message batch, non-null if the batch is sent to a specific member */
    protected Address          dest;

    /** The sender of the message batch */
    protected Address          sender;

    /** The name of the cluster in which the message batch is sent, this is equivalent to TpHeader.cluster_name */
    protected AsciiString      cluster_name;

    /** The storage of the messages; removed messages have a null element */
    protected Message[]        messages;

    /** Index of the next message to be inserted */
    protected int              index;

    /** Whether all messages have dest == null (multicast) or not */
    protected boolean          multicast;

    /** Whether this message batch contains only OOB messages, or only regular messages */
    protected Mode             mode=Mode.REG;

    protected static final int INCR=5; // number of elements to add when resizing
    protected static final     ToIntBiFunction<Message,MessageBatch>  length_visitor=(msg, batch) -> msg != null? msg.getLength() : 0;
    protected static final     ToLongBiFunction<Message,MessageBatch> total_size_visitor=(msg, batch) -> msg != null? msg.size() : 0;

    public MessageBatch() {}

    public MessageBatch(int capacity) {
        this.messages=new Message[capacity];
    }

    public MessageBatch(Collection<Message> msgs) {
        messages=new Message[msgs.size()];
        for(Message msg: msgs)
            messages[index++]=msg;
        determineMode();
    }

    public MessageBatch(Address dest, Address sender, AsciiString cluster_name, boolean multicast, Collection<Message> msgs) {
        this(dest, sender, cluster_name, multicast, msgs, null);
    }

    public MessageBatch(Address dest, Address sender, AsciiString cluster_name, boolean multicast,
                        Collection<Message> msgs, Predicate<Message> filter) {
        messages=new Message[msgs.size()];
        for(Message msg: msgs) {
            if(filter != null && !filter.test(msg))
                continue;
            messages[index++]=msg;
        }
        this.dest=dest;
        this.sender=sender;
        this.cluster_name=cluster_name;
        this.multicast=multicast;
        determineMode();
    }

    public MessageBatch(Address dest, Address sender, AsciiString cluster_name, boolean multicast, Mode mode, int capacity) {
        this(capacity);
        this.dest=dest;
        this.sender=sender;
        this.cluster_name=cluster_name;
        this.multicast=multicast;
        this.mode=mode;
    }

    public Address      getDest()                        {return dest;}
    public Address      dest()                           {return dest;}
    public MessageBatch setDest(Address dest)            {this.dest=dest; return this;}
    public MessageBatch dest(Address dest)               {this.dest=dest; return this;}
    public Address      getSender()                      {return sender;}
    public Address      sender()                         {return sender;}
    public MessageBatch setSender(Address sender)        {this.sender=sender; return this;}
    public MessageBatch sender(Address sender)           {this.sender=sender; return this;}
    public AsciiString  getClusterName()                 {return cluster_name;}
    public AsciiString  clusterName()                    {return cluster_name;}
    public MessageBatch setClusterName(AsciiString name) {this.cluster_name=name; return this;}
    public MessageBatch clusterName(AsciiString name)    {this.cluster_name=name; return this;}
    public boolean      isMulticast()                    {return multicast;}
    public boolean      multicast()                      {return multicast;}
    public MessageBatch multicast(boolean flag)          {multicast=flag; return this;}
    public Mode         getMode()                        {return mode;}
    public Mode         mode()                           {return mode;}
    public MessageBatch setMode(Mode mode)               {this.mode=mode; return this;}
    public MessageBatch mode(Mode mode)                  {this.mode=mode; return this;}
    public int          getCapacity()                    {return messages.length;}
    public int          capacity()                       {return messages.length;}
    public int          index()                          {return index;}


    /** Returns the underlying message array. This is only intended for testing ! */
    public Message[]    array() {
        return messages;
    }

    public <T extends Message> T first() {
        for(int i=0; i < index; i++)
            if(messages[i] != null)
                return (T)messages[i];
        return null;
    }

    public <T extends Message> T last() {
        for(int i=index -1; i >= 0; i--)
            if(messages[i] != null)
                return (T)messages[i];
        return null;
    }

    public MessageBatch add(final Message msg) {
        add(msg, true);
        return this;
    }

    /** Adds a message to the table
     * @param msg the message
     * @param resize whether or not to resize the table. If true, the method will always return 1
     * @return always 1 if resize==true, else 1 if the message was added or 0 if not
     */
    public int add(final Message msg, boolean resize) {
        if(msg == null) return 0;
        if(index >= messages.length) {
            if(!resize)
                return 0;
            resize(index+INCR);
        }
        messages[index++]=msg;
        determineMode(msg);
        return 1;
    }

    public MessageBatch add(final MessageBatch batch) {
        add(batch, true);
        return this;
    }

    /**
     * Adds another batch to this one
     * @param batch the batch to add to this batch
     * @param resize when true, this batch will be resized to accommodate the other batch
     * @return the number of messages from the other batch that were added successfully. Will always be batch.size()
     * unless resize is false: in this case, the number of messages that were added successfully is returned
     */
    public int add(final MessageBatch batch, boolean resize) {
        if(batch == null) return 0;
        if(this == batch)
            throw new IllegalArgumentException("cannot add batch to itself");
        int batch_size=batch.size();
        if(index+batch_size >= messages.length && resize)
            resize(index + batch_size + 1);

        int cnt=0;
        for(Message msg: batch) {
            if(index >= messages.length)
                return cnt;
            messages[index++]=msg;
            cnt++;
        }
        determineMode();
        return cnt;
    }

    /**
     * Adds message to this batch from a message array
     * @param msgs  the message array
     * @param num_msgs the number of messages to add, should be <= msgs.length
     * @param resize whether or not to resize the batch
     * @return the number of messages added to this batch
     */
    public int add(Message[] msgs, int num_msgs, boolean resize) {
        if(msgs == null) return 0;
        if(index+num_msgs >= messages.length && resize)
            resize(index + num_msgs + 1);

        int cnt=0, num_added=0;
        for(Message msg: msgs) {
            if(index >= messages.length || ++num_added > num_msgs)
                return cnt;
            messages[index++]=msg;
            cnt++;
        }
        determineMode();
        return cnt;
    }

    public int add(Collection<Message> msgs, boolean resize) {
        if(msgs == null) return 0;
        int size=msgs.size();
        if(index+size >= messages.length && resize)
            resize(index + size + 1);

        int cnt=0;
        for(Message msg: msgs) {
            if(index >= messages.length)
                return cnt;
            messages[index++]=msg;
            cnt++;
        }
        determineMode();
        return cnt;
    }

    public int add(Message[] msgs, boolean resize) {
        return add(msgs, msgs.length, resize);
    }

    public MessageBatch set(Address dest, Address sender, Message[] msgs) {
        this.messages=Objects.requireNonNull(msgs);
        this.dest=dest;
        this.sender=sender;
        index=msgs.length;
        determineMode();
        return this;
    }

    /**
     * Replaces a message in the batch with another one
     * @param existing_msg The message to be replaced. The message has to be non-null and is found by identity (==)
     *                     comparison
     * @param new_msg The message to replace the existing message with, can be null
     */
    public MessageBatch replace(Message existing_msg, Message new_msg) {
        if(existing_msg == null)
            return this;
        for(int i=0; i < index; i++) {
            if(messages[i] != null && messages[i] == existing_msg) {
                messages[i]=new_msg;
                break;
            }
        }
        return determineMode();
    }

    /**
     * Replaces all messages which match a given filter with a replacement message
     * @param filter the filter. If null, no changes take place. Note that filter needs to be able to handle null msgs
     * @param replacement the replacement message. Can be null, which essentially removes all messages matching filter
     * @param match_all whether to replace the first or all matches
     * @return the MessageBatch
     */
    public MessageBatch replace(Predicate<Message> filter, Message replacement, boolean match_all) {
        replaceIf(filter, replacement, match_all);
        return this;
    }

    /**
     * Replaces all messages that match a given filter with a replacement message
     * @param filter the filter. If null, no changes take place. Note that filter needs to be able to handle null msgs
     * @param replacement the replacement message. Can be null, which essentially removes all messages matching filter
     * @param match_all whether to replace the first or all matches
     * @return the number of matched messages
     */
    public int replaceIf(Predicate<Message> filter, Message replacement, boolean match_all) {
        if(filter == null)
            return 0;
        int matched=0;
        for(int i=0; i < index; i++) {
            if(filter.test(messages[i])) {
                messages[i]=replacement;
                matched++;
                if(!match_all)
                    break;
            }
        }
        determineMode();
        return matched;
    }

    /**
     * Transfers messages from other to this batch. Optionally clears the other batch after the transfer
     * @param other the other batch
     * @param clear If true, the transferred messages are removed from the other batch
     * @return the number of transferred messages (may be 0 if the other batch was empty)
     */
    public int transferFrom(MessageBatch other, boolean clear) {
        if(other == null || this == other)
            return 0;
        int capacity=messages.length, other_size=other.size();
        if(other_size == 0)
            return 0;
        if(capacity < other_size)
            messages=new Message[other_size];
        System.arraycopy(other.messages, 0, this.messages, 0, other_size);
        if(this.index > other_size)
            for(int i=other_size; i < this.index; i++)
                messages[i]=null;
        this.index=other_size;
        if(clear)
            other.clear();
        determineMode();
        return other_size;
    }

    /**
     * Removes the current message (found by indentity (==)) by nulling it in the message array
     * @param msg
     * @return
     */
    public MessageBatch remove(Message msg) {
        return replace(msg, null);
    }

    /**
     * Removes all messages which match filter
     * @param filter the filter. If null, no removal takes place
     * @return the MessageBatch
     */
    public MessageBatch remove(Predicate<Message> filter) {
        return replace(filter, null, true);
    }

    public MessageBatch clear() {
        for(int i=0; i < index; i++)
            messages[i]=null;
        index=0;
        return this;
    }

    public MessageBatch resize(int new_capacity) {
        if(new_capacity <= messages.length)
            return this;
        Message[] tmp=new Message[new_capacity];
        System.arraycopy(messages,0,tmp,0,messages.length);
        messages=tmp;
        return this;
    }


    public MessageBatch reset() {
        index=0;
        return this;
    }

    public boolean anyMatch(Predicate<Message> pred) {
        if(pred == null)
            return false;
        for(int i=0; i < index; i++)
            if(messages[i] != null && pred.test(messages[i]))
                return true;
        return false;
    }


    /** Removes and returns all messages which have a header with ID == id */
    public Collection<Message> getMatchingMessages(final short id, boolean remove) {
        return map((msg, batch) -> {
            if(msg != null && msg.getHeader(id) != null) {
                if(remove)
                    batch.remove(msg);
                return msg;
            }
            return null;
        });
    }


    /** Applies a function to all messages and returns a list of the function results */
    public <T> Collection<T> map(BiFunction<Message,MessageBatch,T> visitor) {
        Collection<T> retval=null;
        for(int i=0; i < index; i++) {
            try {
                T result=visitor.apply(messages[i], this);
                if(result != null) {
                    if(retval == null)
                        retval=new ArrayList<>();
                    retval.add(result);
                }
            }
            catch(Throwable t) {
            }
        }
        return retval;
    }

    public void forEach(BiConsumer<Message,MessageBatch> consumer) {
        for(int i=0; i < index; i++) {
            try {
                consumer.accept(messages[i], this);
            }
            catch(Throwable t) {
            }
        }
    }


    /** Returns the number of non-null messages */
    public int size() {
        int retval=0;
        for(int i=0; i < index; i++)
            if(messages[i] != null)
                retval++;
        return retval;
    }

    public boolean isEmpty() {
        for(int i=0; i < index; i++)
            if(messages[i] != null)
                return false;
        return true;
    }


    /** Returns the size of the message batch (by calling {@link Message#size()} on all messages) */
    public long totalSize() {
        long retval=0;
        for(int i=0; i < index; i++)
            retval+=total_size_visitor.applyAsLong(messages[i], this);
        return retval;
    }

    /** Returns the total number of bytes of the message batch (by calling {@link Message#getLength()} on all messages) */
    public int length() {
        int retval=0;
        for(int i=0; i < index; i++)
            retval+=length_visitor.applyAsInt(messages[i], this);
        return retval;
    }


    /** Iterator which iterates only over non-null messages, skipping null messages */
    public MessageIterator iterator() {
        return new BatchIterator(index, null);
    }

    /** Iterates over all non-null message which match filter */
    public MessageIterator iteratorWithFilter(Predicate<Message> filter) {
        return new BatchIterator(index, filter);
    }

    public Stream<Message> stream() {
        Spliterator<Message> sp=Spliterators.spliterator(iterator(), size(), 0);
        return StreamSupport.stream(sp, false);
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("dest=" + dest);
        if(sender != null)
            sb.append(", sender=").append(sender);
        sb.append(", mode=" + mode);
        if(cluster_name != null)
            sb.append(", cluster=").append(cluster_name);
        if(sb.length() > 0)
            sb.append(", ");
        sb.append(size() + " messages [capacity=" + messages.length + "]");

        return sb.toString();
    }

    public String printHeaders() {
        StringBuilder sb=new StringBuilder().append("dest=" + dest);
        if(sender != null)
            sb.append(", sender=").append(sender);
        sb.append("\n").append(size()).append(":\n");
        int count=1;
        for(Message msg: this)
            sb.append("#").append(count++).append(": ").append(msg.printHeaders()).append("\n");
        return sb.toString();
    }



    public enum Mode {OOB, REG, MIXED}

    protected MessageBatch determineMode() {
        boolean first=true;
        for(int i=0; i < index; i++) {
            if(messages[i] == null)
                continue;
            if(first) {
                mode=messages[i].isFlagSet(Message.Flag.OOB)? Mode.OOB : Mode.REG;
                first=false;
                continue;
            }
            if(mode == Mode.REG && messages[i].isFlagSet(Message.Flag.OOB)
              || mode == Mode.OOB && !messages[i].isFlagSet(Message.Flag.OOB))
                return setMode(Mode.MIXED);
        }
        return this;
    }

    protected MessageBatch determineMode(Message msg) {
        if(msg == null) return this;
        if(index <= 1)
            mode=msg.isFlagSet(Message.Flag.OOB)? Mode.OOB : Mode.REG;
        else {
            if(mode == Mode.REG && msg.isFlagSet(Message.Flag.OOB)
              || mode == Mode.OOB && !msg.isFlagSet(Message.Flag.OOB))
                return setMode(Mode.MIXED);
        }
        return this;
    }



    /** Iterates over <em>non-null</em> elements of a batch, skipping null elements */
    protected class BatchIterator implements MessageIterator {
        protected int                      current_index=-1;
        protected final int                saved_index; // index at creation time of the iterator
        protected final Predicate<Message> filter;

        public BatchIterator(int saved_index, Predicate<Message> filter) {
            this.saved_index=saved_index;
            this.filter=filter;
        }

        public boolean hasNext() {
            // skip null msgs or msgs that don't match the filter (if set)
            while(current_index +1 < saved_index && nullOrNoFilterMatch(current_index+1))
                current_index++;
            return current_index +1 < saved_index;
        }

        public Message next() {
            if(current_index +1 >= messages.length)
                throw new NoSuchElementException();
            return messages[++current_index];
        }

        public void remove() {
            replace(null);
        }

        @Override public void replace(Message msg) {
            if(current_index >= 0)
                messages[current_index]=msg;
        }

        protected boolean nullOrNoFilterMatch(int index) {
            return messages[index] == null ||
              (filter != null && filter.test(messages[index]) == false);
        }
    }


}
