package BayesClassifier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;

import com.etinternational.hamr.DedicatedFlow;
import com.etinternational.hamr.Flow;
import com.etinternational.hamr.KeyValueProducer;
import com.etinternational.hamr.store.kv.HashContainer;
import com.etinternational.hamr.store.kv.KeyValueContainer;
import com.etinternational.hamr.store.kv.KeyValueContainerPartition;
import com.etinternational.hamr.store.kv.KeyValueStore;

/**
 * 自定义KeyValueList的KeyValueStore
 * 输入 k,v 输出 k,{v1,v2,...}
 */
public class KeyValueListStore extends KeyValueStore <String, ArrayList> {

    public KeyValueListStore() {
        super(new HashContainer<>(String.class, ArrayList.class));
    }
    
    private final KeyValueContainer.PushSlot<String, String, String, ArrayList> pushList =
            add(new KeyValueContainer.PushSlot<String, String, String, ArrayList>() {
        @Override
        public void accept(String key, String value, Flow context,
                KeyValueContainerPartition<String, ArrayList> container) 
                throws IOException {
            @SuppressWarnings("unchecked")
            ArrayList<String> list = (ArrayList<String>)container.get(key);
            if (list==null)
                list = new ArrayList();
            list.add(value);
            container.put(key, list);
        }
    });
    
    public KeyValueContainer.PushSlot<String, String, String, ArrayList> pushList() {
        return pushList;
    }
}
