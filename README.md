# PhraseExtract

- Use the following command to search the frequently occurring sentences(the generic options like -files and -D show be placed before the command options):
`hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.8.0.jar \
	-files /home/rav009/PycharmProjects/untitled/PhraseExtract/sentence_mapper.py,/home/rav009/PycharmProjects/untitled/PhraseExtract/sentence_reducer.py \
	-D mapred.map.tasks=7 \
	-D mapred.reduce.tasks=3 \
	-input /input/casetext_LF.txt \
	-output /sentences/above100/ \
	-mapper sentence_mapper.py \
	-reducer sentence_reducer.py`