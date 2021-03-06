package ly.bit.nsq.lookupd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SyncLookupd extends AbstractLookupd {

    private static Logger LOGGER = Logger.getLogger(SyncLookupd.class.getName());

	@Override
	public List<String> query(String topic) {
		try {
			URL url = null;

            if (this.addr.endsWith(("/"))) {
                url = new URL(this.addr + "lookup?topic=" + topic);
            } else {
                url = new URL(this.addr + "/lookup?topic=" + topic);
            }

			InputStream is = url.openStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			return parseResponseForProducers(br);
		} catch (MalformedURLException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
		} catch (IOException e) {
			LOGGER.log(Level.WARNING, e.getMessage(), e);
		}

		return new LinkedList<String>();
	}

	public SyncLookupd(String addr){
		this.addr = addr;
	}

}
