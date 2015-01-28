package fun.messaging.distributed;

import java.util.*;

/**
 * Created by kuldeep
 */
public class RadixTree<T> {

    private final Node<T> root;
    private final ValueWrapperFactory<T> wrapperFactory;

    RadixTree() {
        wrapperFactory = new ValueWrapperFactory<T>() {

            @Override
            public ValueWrapper newInstance(T value) {
                if(value == null)
                    return null;
                return new DefaultValueWrapper(value);
            }
        };
        root = new Node<T>(null, null, wrapperFactory);
        root.children = new Node[2];

    }

    public static class Node<T> {
        public static final int LEFT = 0;
        public static final int RIGHT = 1;
        byte[] key;
        int startBit;
        int endBit;
        Node<T>[] children;
        ValueWrapper<T> val;

        Node(Node<T> other) {
            this.key = other.key;
            this.startBit = other.startBit;
            this.endBit = other.endBit;
            this.children = other.children;
        }

        Node(byte[] key, T val, int startBit, int endBit, Node<T>[] children, ValueWrapperFactory<T> factory) {
            this(key, val, startBit, endBit,factory);
            this.children = children;
        }

        Node(byte[] key, T val, ValueWrapperFactory<T> factory) {
            this.key = key;
            this.val = factory.newInstance(val);
            startBit = 0;
            endBit = 7;
        }

        Node(byte[] key, T val, int startBit, int endBit, ValueWrapperFactory<T> factory) {
            this.key = key;
            this.val = factory.newInstance(val);
            this.startBit = startBit;
            this.endBit = endBit;
        }

        public Node(byte[] key, ValueWrapper<T> value, int startBit, int endBit, Node<T>[] children) {
            this.key = key;
            this.val = value;
            this.startBit = startBit;
            this.endBit = endBit;
            this.children = children;
        }

        void printNode() {
            StringBuffer sb = new StringBuffer();
            printNode(sb, 1);
            System.out.println(sb.toString());
        }

        void printNode(StringBuffer buff, int spaces) {

            char[] c = new char[spaces];
            for (int i = 0; i < spaces; i++) {
                c[i] = '-';
            }

            if (children != null) {
                for (int i = 0; i < children.length; i++) {
                    if (children[i] != null) {
                        buff.append('\n').append(c).append(new String(children[i].key)).
                                append("_").append(children[i].startBit).
                                append("_").append(children[i].endBit).
                                append("_").append(children[i].val == null ? 'N' : 'Y');
                        children[i].printNode(buff, spaces + children[i].key.length + 6);
                    }
                }
            }
        }
    }


    private final void addValueIfAbsent(Node<T> node, T val) {
        if(node.val == null)
            node.val = wrapperFactory.newInstance(val);
        else
            node.val.add(val);

    }
    public void add(byte[] key, T val) {
        synchronized (root) {
            if (key.length == 0 || key == null)
                addValueIfAbsent(root, val);
            int side = getBitValue(key[0], 0);
            if (root.children[side] == null)
                root.children[side] = getNewNode(key, val, 0, 0, 7);
            else
                add(key, val, 0, root.children[side]);
        }
    }


    public final void add(byte[] key, T val, int keyIdx, Node<T> node) {

        while (true) {
            boolean searchFurther = handleFirstByte(node, val, key, keyIdx);
            if (!searchFurther)
                break;

           //KeyIdx incremented inside handle* method
            searchFurther = handleMiddleBytes(node, val, key, keyIdx);
            if (!searchFurther)
                break;

            searchFurther = handLastByte(node, val, key, keyIdx); //Recurse till everything is updated
            if (!searchFurther)
                break;

            keyIdx += node.key.length - 1;
            int nextStartBit = 0;

            if (node.endBit == 7)
                keyIdx++;
            else
                nextStartBit = node.endBit + 1;

            if (keyIdx == key.length) {
                addValueIfAbsent(node,val); //set or overwrite value
                return;
            } else {
                int side = getBitValue(key[keyIdx], nextStartBit);
                if (node.children != null && node.children[side] != null) {
                    node = node.children[side];
                    continue;
                } else {
                    if (node.children == null)
                        node.children = new Node[2];

                    node.children[side] = getNewNode(key, val, keyIdx, nextStartBit, 7);
                    break;
                }
            }
        }
    }

    static final int getBitValue(byte key, int pos) {
        return (key >> (pos)) & 1;
    }


    static final int getMismatchedBit(byte a, byte b, int startBit, int endBit) {
        for (int i = startBit; i <= endBit; i++)
            if (getBitValue(a, i) != getBitValue(b, i))
                return i;

        return -1;
    }

    final Node<T> getNewNode(byte[] key, T val, int keyIdx, int startBit, int endBit) {
        if (keyIdx == 0)
            return new Node<T>(key, val, startBit, endBit, wrapperFactory);

        byte[] dest = new byte[key.length - keyIdx];
        System.arraycopy(key, keyIdx, dest, 0, dest.length);
        return new Node<T>(dest, val, startBit, endBit, wrapperFactory);
    }

    /**
     * Based on the index on the given node, split the given node into two nodes.
     * Children of the new node are set to null and based on the start bit of the new node, new node is referenced
     * to parent node
     *
     * @param node
     * @param keyIdx
     */
    final void splitMultiByteNode(Node<T> node, int keyIdx, int newKeyIdx, int newEndBit) {
        Node<T> tempNode = new Node<T>(Arrays.copyOfRange(node.key, newKeyIdx, node.key.length), node.val,
                newEndBit == 7 ? 0 : newEndBit + 1, node.endBit, node.children);


        node.key = Arrays.copyOfRange(node.key, 0, keyIdx + 1);
        node.endBit = newEndBit;
        resetChildrenAndValue(node, tempNode);
    }

    void splitByteNode(Node<T> node, int newEndBit) {
        Node<T> tempNode = new Node<T>(Arrays.copyOfRange(node.key, 0, node.key.length), node.val,
                newEndBit + 1, node.endBit, node.children);

        node.endBit = newEndBit;
        resetChildrenAndValue(node, tempNode);
    }

    static final <T> void resetChildrenAndValue(Node<T> node, Node<T> tempNode) {
        node.val = null;
        /*
        if(node.children != null)
            for(int i=0; i < node.children.length ;i++)
                node.children[i] = null;
         */
        node.children = new Node[2];
        node.children[getBitValue(tempNode.key[0], tempNode.startBit)] = tempNode;
    }

    /**
     * @return Boolean value indicating if the original node was split
     */
    boolean handleFirstByte(Node<T> node, T val, byte[] key, int keyIdx) {
        byte first = node.key[0];
        int mismatch = getMismatchedBit(first, key[keyIdx], node.startBit, node.key.length == 1 ? node.endBit : 7);
        if (mismatch < 0)   //Perfect Match
            return true;

        //Split node into two and then add two new children(one from existing split node and the other from remaining bytes of the new key)
        splitMultiByteNode(node, 0, 0, mismatch - 1);
        node.children[getBitValue(key[keyIdx], mismatch)] = getNewNode(key, val, keyIdx, mismatch, 7);
        return false;
    }

    final boolean handleMiddleBytes(Node<T> node, T val, byte[] key, int keyIdx) {
        if (node.key.length <= 2)
            return true;
        for (int i = 1; i < node.key.length - 1; i++) {
            int idx = keyIdx + i;
            if (idx >= key.length) {
                splitMultiByteNode(node, i - 1, i, 7);
                addValueIfAbsent(node, val);
                return false;
            } else if (key[idx] != node.key[i]) {
                int mismatch = getMismatchedBit(key[idx], node.key[i], 0, 7);
                if (mismatch == 0)
                    splitMultiByteNode(node, i - 1, i, 7);
                else
                    splitMultiByteNode(node, i, i, mismatch - 1);

                node.children[getBitValue(key[idx], mismatch)] = getNewNode(key, val, idx, mismatch, 7);
                return false;
            }
        }
        return true;
    }


    private final boolean handLastByte(Node<T> node, T val, byte[] key, int keyIdx) {
        keyIdx = keyIdx + node.key.length - 1;
        if (node.key.length > 1) {
            if (keyIdx >= key.length) {
                splitMultiByteNode(node, node.key.length - 2, node.key.length - 1, 7);
                addValueIfAbsent(node, val);
                return false;
            } else {
                byte last = node.key[node.key.length - 1];
                int mismatch = getMismatchedBit(last, key[keyIdx], 0, node.endBit);
                if (mismatch != -1) {
                    if (mismatch == 0)
                        splitMultiByteNode(node, node.key.length - 2, node.key.length - 1, 7);
                    else
                        splitMultiByteNode(node, node.key.length - 1, node.key.length - 1, mismatch-1);

                    node.children[getBitValue(key[keyIdx], mismatch)] = getNewNode(key, val, keyIdx, mismatch, 7);
                    return false;
                }

            }
        }

        return true;
    }


    public static final <T> int matchFirstByte(Node<T> node, byte[] key, int keyIdx) {
        byte first = node.key[0];
        int endBit = node.endBit;
        int incrKeyIdx = 0;
        if (node.key.length > 1 || node.endBit == 7) {
            endBit = 7;
            incrKeyIdx = 1;
        }

        int mismatch = getMismatchedBit(first, key[keyIdx], node.startBit, endBit);
        if (mismatch == -1)   //Perfect Match
            return (keyIdx + incrKeyIdx);

        return -1;
    }

    public static final <T> int matchMiddleBytes(Node<T> node, byte[] key, int keyIdx) {
        if (node.key.length == 2)
            return keyIdx;


        int i = 1;
        int idx = keyIdx;
        for (; i < node.key.length - 1; i++, idx++) {
            if (idx >= key.length)
                return idx;
            else if (key[idx] != node.key[i]) {
                return -1;
            }
        }
        return idx;
    }

    public static final <T> int matchLastBytes(Node<T> node, byte[] key, int keyIdx) {
        if (node.key.length > 1) {
            byte last = node.key[node.key.length - 1];
            int mismatch = getMismatchedBit(last, key[keyIdx], 0, node.endBit);
            if (mismatch != -1)
                return -1;
        }
        return node.endBit == 7 ? ++keyIdx : keyIdx;
    }

    public ValueWrapper<T> get(byte[] key) {
        Node<T> node = root;
        int keyIdx = 0;
        final int side = getBitValue(key[keyIdx], 0);
        if (node.children[side] == null)
            return null;

        node = node.children[side];
        while (true) {
            keyIdx = matchFirstByte(node, key, keyIdx);
            if (keyIdx == -1)
                return null;

            if (keyIdx >= key.length) {
                if (node.key.length == 1 && node.endBit == 7)
                    return node.val;

                return null;
            }

            if (node.key.length == 1) {
                final int nextBit = node.endBit == 7 ? 0 : node.endBit + 1;
                node = node.children[getBitValue(key[keyIdx], nextBit)];
                if (node == null)
                    return null;
                else
                    continue;
            }

            keyIdx = matchMiddleBytes(node, key, keyIdx);
            if (keyIdx == -1 || keyIdx >= key.length)
                return null;

            keyIdx = matchLastBytes(node, key, keyIdx);
            if (keyIdx == -1)
                return null;

            if (keyIdx >= key.length)
                return node.val;

            final int nextBit = node.endBit == 7 ? 0 : node.endBit + 1;
            node = node.children[getBitValue(key[keyIdx], nextBit)];
            if (node == null)
                return null;
        }
    }

    public Collection<T> getMatchesStartingWith (byte[] prefix) {
        Collection<T> list = new ArrayList<>();



        return list;

    }

    public Collection<T> getAllPrefixMatches(byte[] key) {
        return null;
    }



    public void print() {
        root.printNode();


    }


    public static void main(String[] args) {
        int k = 2;
        System.out.println(1>>1);

        RadixTree<String> trie = new RadixTree<>();
        List<String> items = new ArrayList<>();
        /*
        items.add("C");
        items.add("Z");
        items.add("B");
        items.add("X");
        items.add("P");

        items.add("ABC");
        items.add("ABD");

        items.add("ABCD");
        items.add("ABDD");

        items.add("ABE");

        items.add("ABF");
        items.add("ABG");
        items.add("ABH");
        items.add("ABI");
        items.add("ABJ");


        items.add("ABCE");
        items.add("ABDE");
        items.add("CAN");

        items.add("C");
        items.add("Z");
        items.add("B");
        items.add("X");
        items.add("P");

        items.add("PQRSTUVXZ");
        items.add("PQRSTUVWXYZ");
        items.add("PQLSTUVWXMZ");
        items.add("QMSD");
        items.add("YASDASD");

        */
       // trie.print();
        items.add("KBBJDRTOPH");
        /*

        items.add("XJYHMOEQRP");

        items.add("LCDFKGIFUD");
        items.add("VMWDNPORJO");

        items.add("QKGJDXFCPZ");
        items.add("DVHTKYLERV");
        */
        items.add("KNVWOAJFFP");
        /*
        items.add("RYXXIBIAIN");
        items.add("RNMVWYZMWE");
        items.add("RCOUWAGSFP");
        items.add("RJWPISCTCD");
        items.add("TZWVCGTSSB");
        items.add("VSXTABCDWR");
        items.add("SJPRSIZNGA");
        items.add("NXXOOBIPRM");
        items.add("ZWAFMMWBGM");
        */
        items.add("KDKLHACMQL");
       // items.add("ESXMXVIBSA");
        //items.add("NKQOTNUBWF");
        //items.add("DOBTDNKUPI");
        //items.add("KBBJDRTOPH");

        for (String item : items) {
            trie.add(item.getBytes(), item);
            if(trie.get(item.getBytes()) == null)
                trie.root.printNode();
        }

        trie.root.printNode();

        for (String item : items)
            if (trie.get(item.getBytes()) == null || !item.equals(trie.get(item.getBytes()).get())) {
                System.out.println(item);
                trie.get(item.getBytes());
                //System.out.println(new String(trie.get(item.getBytes())));
            }

        //System.out.println(trie.get("PQLSTUVWXMZ".getBytes()));


        //trie.root.printNode();
    }
}
