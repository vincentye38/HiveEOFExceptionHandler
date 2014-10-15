import java.io.EOFException;
import java.io.IOException;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

import org.apache.hadoop.hive.io.HiveIOExceptionHandler;
import org.apache.hadoop.hive.io.HiveIOExceptionNextHandleResult;
import org.apache.hadoop.mapred.RecordReader;


public class EOFExceptionHandler implements HiveIOExceptionHandler {

	@Override
	public RecordReader<?, ?> handleRecordReaderCreationException(Exception e)
			throws IOException {

		return new RecordReader() {

			@Override
			public void close() throws IOException {
				// TODO Auto-generated method stub

			}

			@Override
			public Object createKey() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Object createValue() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public long getPos() throws IOException {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public float getProgress() throws IOException {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public boolean next(Object arg0, Object arg1) throws IOException {
				// TODO Auto-generated method stub
				return false;
			}
		};

	}

	@Override
	public void handleRecorReaderNextException(Exception e,
			HiveIOExceptionNextHandleResult result) throws IOException {
		if (e instanceof EOFException){
			result.setHandled(true);
			result.setHandleResult(false);
		}
		
	}

	

}
