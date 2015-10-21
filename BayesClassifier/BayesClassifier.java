package BayesClassifier;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.etinternational.hamr.Flow;
import com.etinternational.hamr.HAMR;
import com.etinternational.hamr.Workflow;
import com.etinternational.hamr.resource.ResourceReader;
import com.etinternational.hamr.resource.ResourceWriter;
import com.etinternational.hamr.resource.file.FileResourceReader;
import com.etinternational.hamr.resource.file.FileResourceWriter;
import com.etinternational.hamr.sim.SimulationConfig;
import com.etinternational.hamr.store.kv.number.NumberStore;
import com.etinternational.hamr.transform.Transform;

public class BayesClassifier {
	private static final String KV_OUTPUT_FORMAT = "%1$s : %2$s\n";
	// 命令行参数
    @Option(name="--help",usage="print this message",help=true)
    private boolean displayUsage = false;

    @Argument(usage="input file (REQUIRED)",required=true,metaVar="FILE")    
    private String emailFile = null;
    
    @Option(name="-s",required=true,metaVar="FILE")
    private String spamFile = null;
    
    @Option(name="-o",required=true,metaVar="FILE")
    private String outputFile = null;

	void run() throws Exception{
		// 暂无输出
		ResourceWriter<String, Long> writer = null;
		
		try {
			// 初始化 HAMR 系统
			HAMR.initialize(new SimulationConfig());
					
			// 创建一个 workflow
			Workflow workflow = new Workflow();
			// TODO 不设置为1有时会报错 java.lang.NullPointerException
			// 必须将bind(wordCount).broadcast，否则可能出现某种划分没有E或S
			// 使得 pull E或S 返回 value 为 null的情况
			// workflow.setDefaultPartitionCount(1);
			
			// 从本地文件系统中读取文件到 KV 对中
			ResourceReader<Long, String> emailReader = 
					new FileResourceReader<>(emailFile);
			ResourceReader<Long, String> spamReader = 
					new FileResourceReader<>(spamFile);
			// 将结果写入本地文件
			writer = new FileResourceWriter<>(outputFile, KV_OUTPUT_FORMAT);
			
			// 创建一个transfrom，将读取的 email 文件中的 word 打上标签"E"输出并给标签计数
			TagAndCount tagAndCountE = new TagAndCount("E");

			// 创建一个transfrom，将读取的 spam 文件中的 word 打上标签"S"输出并给标签计数
			TagAndCount tagAndCountS = new TagAndCount("S");
			
			// 创建List 保存每个word 的分类列表
			final KeyValueListStore wordClassAccumulator = new KeyValueListStore();
			
			// 创建 NumberStore 计算 Email 和 SPAM 中单词的个数
			final NumberStore<String, Long> wordCount = 
					new NumberStore<>(String.class, Long.class);

			// 输出每个word 的分类列表
			final Transform<String, ArrayList, String, Long> output = 
					new Transform<String, ArrayList, String, Long>() {
				@Override
				protected void apply(String key, ArrayList value, Flow flow) 
						throws IOException {
					// pull Email 和 SPAM 中单词的个数
					Long wordNumberE = flow.pull("E");
					Long wordNumberS = flow.pull("S");
					// 统计每个单词出现在 E 和 S 中的次数
					float occurrencesE = Collections.frequency(value, "E");
					float occurrencesS = Collections.frequency(value, "S");
					
//					for (Object v: value)
//						System.out.print(v);
//					System.out.println(" E=" + occurrencesE + "/" + wordNumberE 
//							+ " S=" + occurrencesS + "/" + wordNumberS);
					
					flow.push(key, occurrencesE + "/" + wordNumberE 
							+ " | " + occurrencesS + "/" + wordNumberS);
				}
			};
			
			// 添加到 workflow
			workflow.add(emailReader, spamReader, tagAndCountE, tagAndCountS, 
					wordClassAccumulator, wordCount, output, writer);
			
			// bind DAG
			emailReader.bindPush(tagAndCountE).synchronous();
			spamReader.bindPush(tagAndCountS).synchronous();
			// 使用KeyValueListStore 的pushList入口(PushSlot)
			// 统计来自 E和S的 TagAndCount的pushWordTag出口(PushPort)的单词标记
			tagAndCountE.pushWordTag.bind(wordClassAccumulator.pushList());
			tagAndCountS.pushWordTag.bind(wordClassAccumulator.pushList());
			// 使用NumberStore 的sum 入口统计来着 E和S 的单词个数
			// 使用broadcast方式保证单词个数对所有任务一致
			tagAndCountE.pushWordCount.bind(wordCount.sum()).broadcast();
			tagAndCountS.pushWordCount.bind(wordCount.sum()).broadcast();
			// 计算每个单词在 E和S 中出现的频率
			output.bindPull(wordCount);
			wordClassAccumulator.bindPush(output);
			output.bindPush(writer);
			// 结果写入一个文件
//			writer.setPartitionCount(1);
			
			// 执行workflow
			workflow.execute();		
		} finally {
			if (HAMR.isInitialized())
				HAMR.shutdown();
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		BayesClassifier example = new BayesClassifier();
        CmdLineParser parser = new CmdLineParser(example);
        parser.setUsageWidth(80);

        try {
            parser.parseArgument(args);
            if (example.displayUsage) {
                example.printUsage(parser, System.out, null);
                System.exit(0);
            }
        } catch (Exception e) {
            example.printUsage(parser, System.err, "ERROR: " + e.getMessage());
            throw e;
        }
        
        example.run();
	}
	
    /**
     * Print usage and optional message to specified stream.
     * 
     * @param parser instance of command line parser
     * @param stream output stream to use (system out or err)
     */
    void printUsage(CmdLineParser parser, PrintStream stream, String message) {
        if (message != null) {
            stream.println(message);
        }
        stream.println(this.getClass().getSimpleName() + " --help | FILE");
        parser.printUsage(stream);
        stream.println();
    }
}


