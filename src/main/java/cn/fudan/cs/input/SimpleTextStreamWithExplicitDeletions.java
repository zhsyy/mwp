package cn.fudan.cs.input;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTextStreamWithExplicitDeletions extends TextFileStream<Integer, Integer, String> {

    private final Logger logger = LoggerFactory.getLogger(TextFileStream.class);

    public InputTuple<Integer, Integer, String> next() {
        InputTuple<Integer, Integer, String> tuple = new InputTuple<Integer, Integer, String>(null, null ,null, 0);
        String line;
        try {
            while((line = bufferedReader.readLine()) != null && maxSize >= 0) {
                maxSize--;
                int i = parseLine(line);
                // only if we fully
                if(i == 4 || i == 5) {
                    setSource(tuple);
                    setLabel(tuple);
                    setTarget(tuple);
                    updateCurrentTimestamp();
                    setTimestamp(tuple);
                    localCounter++;
                    globalCounter++;
                    if (i == 5){
                        setDeletion(tuple);
                    }
                    break;
                } else {
                    logger.error("Parsing input line error: {}", line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
//            logger.error("Parsing input line: {}", line, e);
            return null;
        }
        if (line == null || maxSize < 0) {
//            logger.error("Parsing input line: null");
            return null;
        }

        return tuple;
    }


    @Override
    protected int getRequiredNumberOfFields() {
        return 4;
    }

    @Override
    protected void setSource(InputTuple<Integer, Integer, String> tuple) {
        tuple.setSource(Integer.parseInt(splitResults[0]));
    }

    @Override
    protected void setTarget(InputTuple<Integer, Integer, String> tuple) {
        tuple.setTarget(Integer.parseInt(splitResults[2]));
    }

    @Override
    protected void setDeletion(InputTuple<Integer, Integer, String> tuple) {
        tuple.setIfDeletion(true);
    }

    @Override
    protected void setLabel(InputTuple<Integer, Integer, String> tuple) {
        tuple.setLabel(splitResults[1]);
    }

    @Override
    protected void updateCurrentTimestamp() {
        lastTimestamp = globalCounter;
    }

    @Override
    protected void setTimestamp(InputTuple<Integer, Integer, String> tuple) {
        tuple.setTimestamp(Long.parseLong(splitResults[3]));
    }

    public void reset() {
        close();

        open(this.filename);

        localCounter = 0;
        globalCounter = 0;
    }
}
