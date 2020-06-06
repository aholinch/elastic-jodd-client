package es.jodd.client;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import jodd.http.HttpBrowser;
import jodd.http.HttpRequest;
import jodd.http.HttpResponse;
import jodd.json.JsonArray;
import jodd.json.JsonObject;
import jodd.json.JsonParser;

/**
 * A light-weight client for talking to ElasticSearch servers.
 * 
 * @author aholinch
 *
 */
public class ElasticClient 
{
	/**
	 * The JODD HttpBrowser we use as a client.
	 */
	protected HttpBrowser httpClient;

	/**
	 * The baseURL that combines protocol, hostname, and port.
	 */
	protected String baseURL;

	/**
	 * The max number of operations in a single request
	 */
	protected int bulkBatchSize = 10000;

	/**
	 * The max number of ids to delete in a single request
	 */
	protected int deleteByIdBatchSize = 500;

	/**
	 * The type of HTTP authentication
	 */
	protected String authType = "none";

	protected String username = "elastic";

	protected String password = null;

	private String cachedHeader = null;

	/**
	 * The default ElasticSearch URL, http://localhost:9200/.
	 */
	public static final String LOCAL_HOST = "http://localhost:9200/";

	/**
	 * Java logger.
	 */
	private static final Logger logger = Logger.getLogger(ElasticClient.class.getName());


	/**
	 * Default constructor.
	 */
	public ElasticClient()
	{
		init();
	}

	/**
	 * Constructor with URL information.
	 * 
	 * @param baseURL
	 */
	public ElasticClient(String baseURL)
	{
		this.baseURL = baseURL;
		init();
	}

	/**
	 * Initialize the internal http client.
	 */
	protected void init()
	{
		setBaseURL(LOCAL_HOST);

		httpClient = new HttpBrowser();
	}

	protected HttpResponse sendRequest(HttpRequest req)
	{
		if(authType != null && authType.equals("basic"))
		{
			if(cachedHeader == null)
			{
				String auth = username+":"+password;
				auth = jodd.util.Base64.encodeToString(auth);
				auth = "Basic "+auth;
				cachedHeader = auth;
			}
			req.header("Authorization", cachedHeader);
		}
		return httpClient.sendRequest(req);
	}

	/**
	 * Returns the URL.
	 * 
	 * @return
	 */
	public String getBaseURL()
	{
		return baseURL;
	}

	/**
	 * The base URL combines protocol (http|https), hostname, and port number.  The default is http://localhost:9200/.
	 * 
	 * @param url
	 */
	public void setBaseURL(String url)
	{
		baseURL = url;
		if(baseURL == null || baseURL.trim().length() == 0)
		{
			baseURL = LOCAL_HOST;
		}

		if(!baseURL.endsWith("/"))
		{
			baseURL += "/";
		}
	}

	public int getBulkBatchSize()
	{
		return bulkBatchSize;
	}

	public void setBulkBatchSize(int batch)
	{
		bulkBatchSize = batch;
	}

	public void setUsername(String user)
	{
		username = user;
	}

	public String getUsername()
	{
		return username;
	}

	public void setAuthType(String type)
	{
		if(type != null)
		{
			type = type.toLowerCase();
		}

		authType = type;
	}

	public String getAuthType()
	{
		return authType;
	}

	public void setPassword(String secret)
	{
		password = secret;
	}

	public String getPassword()
	{
		// not available;
		return null;
	}

	/**
	 * Returns the list of index names.
	 * 
	 * @return
	 */
	public List<String> getIndexNames() throws ECException
	{
		String url = baseURL +"_cat/indices?format=json";
		logger.info(url);

		HttpRequest req = HttpRequest.get(url);

		HttpResponse resp = sendRequest(req);

		logResponse("Index name",resp);

		JsonParser jp = new JsonParser();

		JsonArray arr = jp.parseAsJsonArray(resp.body());

		List<String> names = null;

		if(arr != null && arr.size()>0)
		{
			int size = arr.size();
			names = new ArrayList<String>(size);
			JsonObject obj = null;

			for(int i=0; i<size; i++)
			{
				obj = arr.getJsonObject(i);
				names.add(obj.getString("index"));
			}
		}
		return names;
	}

	public Map<String,String> getMappings(String index) throws ECException
	{
		Map<String,String> types = new HashMap<String,String>();

		String url = baseURL + index + "/";
		logger.info(url);
		HttpRequest req = HttpRequest.get(url);
		HttpResponse resp = sendRequest(req);

		logResponse("mappings",resp);
		JsonParser jp = new JsonParser();

		JsonObject obj = jp.parseAsJsonObject(resp.body());
		obj = obj.getJsonObject(index);
		obj = obj.getJsonObject("mappings");

		//Map<String,Object> m1 = obj.map();
		Map<String,Object> m2 = null;
		Map<String,String> mt = null;

		//List<String> typeNames = new ArrayList<String>(m1.keySet());
		//Collections.sort(typeNames);
		//int size = typeNames.size();

		List<String> ps = null;
		String p = null;
		int np = 0;

		String type = null;
		//JsonObject o1 = null;
		JsonObject o2 = null;

		type = index;
		// for kibana >= 7  there is only one type per index
		//for(int i=0; i<size; i++)
		{
			//type = typeNames.get(i);
			//o1 = obj.getJsonObject(type);
			mt = new HashMap<String,String>();
			//o2 = o1.getJsonObject("properties");
			o2 = obj.getJsonObject("properties");
			m2 = o2.map();
			ps = new ArrayList<String>(m2.keySet());
			Collections.sort(ps);
			np = ps.size();

			for(int j=0; j<np; j++)
			{
				p = ps.get(j);
				type = o2.getJsonObject(p).getString("type");
				mt.put(p, type);
			}
		}

		types = mt;

		return types;
	}


	/**
	 * Run a match query for the specified value of the field against the index.  Defaults to 10 maxHits.
	 * 
	 * @param index
	 * @param field
	 * @param value
	 * @return
	 */
	public String runMatchQueryRaw(String index, String field, String value) throws ECException
	{
		return runMatchQueryRaw(index,field,value,10);
	}

	/**
	 * Run a match query for the specified value of the field against the index.
	 * 
	 * @param index
	 * @param field
	 * @param value
	 * @param maxHits
	 * @return
	 */
	public String runMatchQueryRaw(String index, String field, String value, int maxHits) throws ECException
	{
		String url = baseURL + index+"/_search";

		if(maxHits == 0)
		{
			url = baseURL+index+"/_count";
		}

		logger.info(url);
		HttpRequest req = HttpRequest.get(url);

		if(field != null)
		{
			if(value != null)
			{
				value = escapeJSON(value);

				value = "\""+value+"\"";
			}

			String body = null;

			if(maxHits > 0)
			{
				body = "{\"size\":"+String.valueOf(maxHits)+",\"query\":{\"match\":{\""+field+"\":"+value+"}}}";
			}
			else
			{
				body = "{\"query\":{\"match\":{\""+field+"\":"+value+"}}}";
			}
			logger.info(body);

			req.bodyText(body, "application/json");
		}
		else if(maxHits > -1)
		{
			String body = "{\"size\":"+String.valueOf(maxHits)+"}";
			logger.info(body);

			req.bodyText(body, "application/json");
		}

		HttpResponse resp = sendRequest(req);

		logResponse("Query response",resp);

		String body = resp.body();

		return body;
	}

	/**
	 * Run a match query for the specified value of the field against the index.
	 * 
	 * @param index
	 * @param field
	 * @param value
	 * @param maxHits
	 * @return
	 */
	public String runQueryStringQueryRaw(String index, String query, int maxHits) throws ECException
	{
		String url = baseURL + index+"/_search";
		if(maxHits == 0)
		{
			url = baseURL + index + "/_count";
		}
		logger.info(url);
		HttpRequest req = HttpRequest.get(url);

		if(query != null)
		{
			query = escapeJSON(query);
			String body = null;
			if(maxHits > 0)
			{
				body = "{\"size\":"+String.valueOf(maxHits)+",\"query\":{\"query_string\":{\"query\": \""+query+"\"}}}";
			}
			else
			{
				body = "{\"query\":{\"query_string\":{\"query\": \""+query+"\"}}}";    			
			}
			logger.info(body);

			req.bodyText(body, "application/json");
		}
		else if(maxHits > -1)
		{
			String body = "{\"size\":"+String.valueOf(maxHits)+"}";
			logger.info(body);

			req.bodyText(body, "application/json");
		}

		HttpResponse resp = sendRequest(req);

		logResponse("Query response",resp);

		String body = resp.body();

		return body;
	}

	/**
	 * Utility method for logging response status.
	 * 
	 * @param string
	 * @param resp
	 */
	protected void logResponse(String msg, HttpResponse resp)  throws ECException
	{
		if(resp != null)
		{
			logger.info(msg + ": " + resp.statusCode() + ", " + resp.statusPhrase());
			int code = resp.statusCode();
			code = (int)(code/100);
			if(code != 2)
			{
				ECException ec = new ECException(resp.statusPhrase());
				ec.setHttpStatus(resp.statusCode());
				ec.setResponseBody(resp.body());
				throw ec;
			}
		}
		else
		{
			logger.warning("Null response object");
		}
	}

	/**
	 * Run a match query for the specified value of the field against the index and parse the results.
	 * 
	 * @param index
	 * @param field
	 * @param value
	 * @return
	 */
	public SearchResults runMatchQuery(String index, String field, String value) throws ECException
	{
		return runMatchQuery(index,field,value,10);
	}

	/**
	 * Run a match query for the specified value of the field against the index and parse the results.
	 * 
	 * @param index
	 * @param field
	 * @param value
	 * @param maxHits
	 * @return
	 */
	public SearchResults runMatchQuery(String index, String field, String value, int maxHits) throws ECException
	{
		String body = runMatchQueryRaw(index,field,value,maxHits);
		SearchResults res = parseElasticSearchResponse(body,maxHits);

		return res;
	}

	public SearchResults runQueryStringQuery(String index, String query, int maxHits) throws ECException
	{
		String body = runQueryStringQueryRaw(index,query,maxHits);
		SearchResults res = parseElasticSearchResponse(body,maxHits);

		return res;
	}

	/**
	 * Parse the provided json string into a search results object.
	 * 
	 * @param jsonStr
	 * @return
	 */
	public SearchResults parseElasticSearchResponse(String jsonStr, int maxHits)
	{
		SearchResults res = null;

		try
		{
			JsonParser jp = new JsonParser();
			JsonObject obj = jp.parseAsJsonObject(jsonStr);
			JsonObject orig = obj;
			obj = obj.getJsonObject("hits");

			SearchHit[] hits = null;

			res = new SearchResults();
			if(obj != null)
			{
				// total used to be a simple integer
				try
				{
					// but may be more complicated now
					res.setTotal(obj.getJsonObject("total").getInteger("value"));
				}
				catch(Exception ex)
				{
					// fall back to integer
					res.setTotal(obj.getInteger("total"));
				}

				if(res.getTotal()>0 && maxHits > 0)
				{
					res.setMaxScore(obj.getDouble("max_score"));

					JsonArray arr = obj.getJsonArray("hits");
					int size = arr.size();
					hits = new SearchHit[size];

					res.setHits(hits);

					SearchHit hit = null;

					for(int i=0; i<size; i++)
					{
						obj = arr.getJsonObject(i);
						hit = new SearchHit();
						hits[i]=hit;
						hit.setID(obj.getString("_id"));
						hit.setScore(obj.getDouble("_score"));
						obj = obj.getJsonObject("_source");
						hit.setSourceObject(obj);
						//hit.setSource(obj.toString());
						//System.out.println(hit.getID() + "\t" + hit.getScore()+"\t"+hit.getSource());
					}

				} // end total > 0

			} // end obj != null
			else
			{
				Long cnt = orig.getLong("count");
				res.setTotal(cnt);
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING, "Error parsing results", ex);
		}
		return res;
	}

	/**
	 * Fetch the json document with the specified id.
	 * 
	 * @param index
	 * @param id
	 * @return
	 */
	public String getDoc(String index, String id) throws ECException
	{
		if(id == null || id.trim().length() == 0) return null;

		String json = null;
		try
		{
			String url = baseURL + index+"/_doc/"+id;

			logger.info(url);

			HttpRequest req = null;

			req = HttpRequest.get(url);

			HttpResponse resp = sendRequest(req);

			logResponse("get response",resp);

			json = resp.body();
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting doc",ex);
			if(ex instanceof ECException) throw ((ECException)ex);
		}

		return json;
	}

	/**
	 * If ID is not null it is used, otherwise an ID is generated and returned.  It will overwrite existing doc with same id.
	 * 
	 * @param index
	 * @param jsonStr
	 * @param id
	 * @return
	 */
	public String saveDoc(String index, String jsonStr, String id) throws ECException
	{
		return saveDoc(index,jsonStr,id,-1,-1);
	}


	public String saveDoc(String index, String jsonStr, String id, int seq_no, int primary_term) throws ECException
	{
		if(jsonStr == null || jsonStr.trim().length() == 0) return null;

		String outID = null;

		try
		{
			String url = baseURL + index+"/_doc/";

			boolean doPut = false;
			if(id != null && id.trim().length() > 0)
			{
				doPut = true;
				id = id.trim();
				outID = id;
				url +=id;

				if(seq_no > -1 && primary_term > -1)
				{
					url+="?if_seq_no="+seq_no+"&if_primary_term="+primary_term;
				}
			}
			logger.info(url);

			HttpRequest req = null;

			if(doPut)
			{
				req = HttpRequest.put(url);
			}
			else
			{
				req = HttpRequest.post(url);
			}

			req.bodyText(jsonStr, "application/json");

			HttpResponse resp = sendRequest(req);

			logResponse("save response",resp);

			String body = resp.body();
			JsonParser jp = new JsonParser();
			JsonObject obj = jp.parseAsJsonObject(body);
			outID = obj.getString("_id");
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error saving doc",ex);
			if(ex instanceof ECException) throw ((ECException)ex);
		}

		return outID;
	}

	/**
	 * Use the _bulk api to create new docs from json.
	 * 
	 * @param index
	 * @param jsonStrs
	 * @return
	 */
	public List<String> multiCreateDoc(String index, List<String> jsonStrs) throws ECException
	{
		List<String> ids = null;
		try
		{
			int size = jsonStrs.size();
			ids = new ArrayList<String>(size);

			String url = baseURL + index+"/_bulk";

			logger.info(url);

			HttpRequest req = null;

			String jsonCMD = "{ \"index\" : { \"_index\" : \""+index+"\"} }\n";

			int ind = 0;

			int est = size;
			String jsonRet = null;
			if(size > bulkBatchSize) est = bulkBatchSize;
			StringBuilder sb = null;

			int remain = size;
			int tgt = 0;

			JsonParser parser = new JsonParser();
			JsonArray arr = null;
			JsonObject obj = null;

			while(ind < size)
			{
				req = HttpRequest.post(url);

				sb = new StringBuilder(est*100);
				if(remain > bulkBatchSize) tgt = bulkBatchSize;
				else tgt = remain;

				for(int i=0; i<tgt; i++)
				{
					sb.append(jsonCMD);
					sb.append(jsonStrs.get(ind)).append("\n");
					ind++;
				}

				req.bodyText(sb.toString(), "application/x-ndjson");

				HttpResponse resp = sendRequest(req);

				logResponse("bulk response",resp);

				jsonRet = resp.body();

				obj = parser.parseAsJsonObject(jsonRet);

				arr = obj.getJsonArray("items");
				for(int i=0; i<tgt; i++)
				{
					obj = arr.getJsonObject(i);
					ids.add(obj.getJsonObject("index").getString("_id"));
				}
				remain-=tgt;
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error doing bulk create",ex);
			if(ex instanceof ECException) throw ((ECException)ex);
		}

		return ids;
	}

	public void multiSaveDoc(String index, List<String> jsonStrs, List<String> ids) throws ECException
	{
		try
		{
			int size = jsonStrs.size();
			String url = baseURL + index+"/_bulk";

			logger.info(url);

			HttpRequest req = null;

			String jsonCMD = "{ \"index\" : { \"_index\" : \""+index+"\",\"_id\":\"";
			String cmd2 = "\"}}\n";
			int ind = 0;

			int est = size;
			if(size > bulkBatchSize) est = bulkBatchSize;
			StringBuilder sb = null;

			int remain = size;
			int tgt = 0;

			String id = null;

			while(ind < size)
			{
				req = HttpRequest.post(url);

				sb = new StringBuilder(est*100);
				if(remain > bulkBatchSize) tgt = bulkBatchSize;
				else tgt = remain;

				for(int i=0; i<tgt; i++)
				{
					id = ids.get(ind);
					sb.append(jsonCMD).append(id).append(cmd2);
					sb.append(jsonStrs.get(ind)).append("\n");
					ind++;
				}

				req.bodyText(sb.toString(), "application/x-ndjson");

				HttpResponse resp = sendRequest(req);

				logResponse("bulk response",resp);

				remain-=tgt;
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error doing bulk save",ex);
			if(ex instanceof ECException) throw ((ECException)ex);
		}
	}

	public void deleteDoc(String index, String id) throws ECException
	{
		if(id == null || id.trim().length() == 0) return;
		if(index == null || index.trim().length() == 0) return;

		try
		{
			String url = baseURL + index+"/_doc/"+id;

			logger.info(url);

			HttpRequest req = null;

			req = HttpRequest.delete(url);

			HttpResponse resp = sendRequest(req);

			logResponse("get response",resp);

		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error deleting doc",ex);
			if(ex instanceof ECException) throw ((ECException)ex);
		}
	}

	public void deleteDocs(String index, List<String> ids) throws ECException
	{
		/*
        "query": {
        	"ids" : {
            "values" : ["1", "4", "100"]
        	}
    	}
		 */
	}

	public void deleteAllDocs(String index) throws ECException
	{
		if(index == null || index.trim().length() == 0) return;

		String url = baseURL + index+"/_delete_by_query?conflicts=proceed";

		logger.info(url);
		HttpRequest req = HttpRequest.post(url);

		String body = null;

		body = "{\"query\":{\"match_all\": {}}}";	
		logger.info(body);

		req.bodyText(body, "application/json");

		HttpResponse resp = sendRequest(req);

		logResponse("delete all response",resp);
	}

	public Map<String,Long> getSimpleAggregate(String index, String keywordField) throws ECException
	{
		String url = baseURL + index+"/_search";
		logger.info(url);
		HttpRequest req = HttpRequest.get(url);

		if(keywordField != null)
		{
			if(!keywordField.endsWith(".keyword"))keywordField+=".keyword";

			String body = "{\"size\":\"0\",\"aggs\":{\"keyagg\":{\"terms\":{\"field\": \""+keywordField+"\"}}}}";
			logger.info(body);

			req.bodyText(body, "application/json");
		}

		Map<String,Long> m = null;

		try
		{
			HttpResponse resp = sendRequest(req);

			logResponse("Query response",resp);

			String body = resp.body();
			JsonObject obj = JsonParser.create().parseAsJsonObject(body);

			obj = obj.getJsonObject("aggregations");
			obj = obj.getJsonObject("keyagg");

			m = new HashMap<String,Long>();

			JsonArray arr = obj.getJsonArray("buckets");

			String key = null;
			Long cnt = null;

			for(int i=0; i<arr.size(); i++)
			{
				obj = arr.getJsonObject(i);
				key = obj.getString("key");
				cnt = obj.getLong("doc_count");
				m.put(key, cnt);
			}
		}
		catch(Exception ex)
		{
			logger.log(Level.WARNING,"Error getting aggregation",ex);
			throw new ECException("Error getting aggregation",ex);
		}
		return m;	
	}

	public String escapeJSON(String str)
	{
		if(str == null || str.length() == 0) return str;

		int len = str.length();
		char b;
		char c = 0;
		String hhhh;
		StringWriter w = new StringWriter((int)(1.1*len));
		for (int i = 0; i < len; i += 1) {
			b = c;
			c = str.charAt(i);
			switch (c) {
			case '\\':
			case '"':
				w.write('\\');
				w.write(c);
				break;
			case '/':
				if (b == '<') {
					w.write('\\');
				}
				w.write(c);
				break;
			case '\b':
				w.write("\\b");
				break;
			case '\t':
				w.write("\\t");
				break;
			case '\n':
				w.write("\\n");
				break;
			case '\f':
				w.write("\\f");
				break;
			case '\r':
				w.write("\\r");
				break;
			default:
				if (c < ' ' || (c >= '\u0080' && c < '\u00a0')
						|| (c >= '\u2000' && c < '\u2100')) {
					w.write("\\u");
					hhhh = Integer.toHexString(c);
					w.write("0000", 0, 4 - hhhh.length());
					w.write(hhhh);
				} else {
					w.write(c);
				}
			}
		}

		return w.toString();
	}

}
