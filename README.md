# PhraseExtract

- Use the following command to search the frequently occurring sentences(the generic options like -files and -D show be placed before the command options):

```
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.8.0.jar \
	-files /home/rav009/PycharmProjects/untitled/PhraseExtract/sentence_mapper.py,/home/rav009/PycharmProjects/untitled/PhraseExtract/sentence_reducer.py \
	-D mapred.map.tasks=7 \
	-D mapred.reduce.tasks=3 \
	-input /input/text.txt \
	-output /sentences/above100/ \
	-mapper "python sentence_mapper.py" \
	-reducer "python sentence_reducer.py -t 100"
```

`python sentence_reducer.py -t 100` stands for output all the sentence appears for more than 100 times.


- Use the following command to search the frequently occurring phrases which contains 2 or 3 words:

```
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.8.0.jar \
	-files /home/rav009/PycharmProjects/untitled/PhraseExtract/phrase_mapper.py,/home/rav009/PycharmProjects/untitled/PhraseExtract/phrase_reducer.py,hdfs://127.0.0.1:9000/sentences/above100/part-00000 \
	-D mapred.map.tasks=7 \
	-D mapred.reduce.tasks=3 \
	-input /input/text.txt \
	-output /phrase/above2000 \
	-mapper "python phrase_mapper.py -l 3" \
	-reducer "python phrase_reducer.py -t 2000 -c"
```

`python phrase_mapper.py -l 3` stands for generate the phrases contain less than or equal to 3 words.  
`python phrase_reducer.py -t 2000 -c` stands the threshold of frequency of phrase is 2000 and also output the case number.