package BayesClassifier;

import java.io.IOException;

import com.etinternational.hamr.Flow;
import com.etinternational.hamr.Flowlet;
import com.etinternational.hamr.PushPort;
import com.etinternational.hamr.PushSlot;

public class TagAndCount extends Flowlet {
	/**
	 * @param tag 用于给不同来源的单词打标，正常邮件为"E"，垃圾邮件为"S"
	 */
	public TagAndCount(final String tag) {		
		super();
		add(new PushSlot<Long, String>() {
			@Override
			public void accept(Long key, String value, Flow flow) 
					throws IOException {
				// 统计 Tag "E" / "S" 
				flow.push(pushWordCount, tag, 1L);
				// 给每个 Word 打上 Tag
				flow.push(pushWordTag, value, tag);
			}
		});	
	}
	// 增加KV 出口（PushPort） pushWordCount 用于对每个单词计数
	public PushPort<String, Long> pushWordCount = add(new PushPort<String, Long>());
	// 增加KV 出口（PushPort） pushWordTag 用于给每个单词打标
	public PushPort<String, String> pushWordTag = add(new PushPort<String, String>());
	
}
