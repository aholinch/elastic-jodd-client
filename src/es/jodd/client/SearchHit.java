package es.jodd.client;

import jodd.json.JsonObject;

/**
 * A search hit holds the search score and JSON object.
 * 
 * @author aholinch
 *
 */
public class SearchHit 
{
	/**
	 * The query score.
	 */
	protected double score;
	
	/**
	 * The object source json as a string.
	 */
	protected String source;
	
	/**
	 * The index id.
	 */
	protected String id;
	
	/**
	 * The source object as a JODD json object.
	 */
	protected JsonObject obj;
	
	/**
	 * Default constructor.
	 */
	public SearchHit()
	{
		
	}
	
	public String getID()
	{
		return id;
	}
	
	public void setID(String str)
	{
		id = str;
	}
	
    public double getScore()
    {
    	return score;
    }
    
    public void setScore(double val)
    {
    	score = val;
    }
    
    public String getSource()
    {
    	if(source == null && obj != null)
    	{
    		source = obj.toString();
    	}
    	return source;
    }
    
    public void setSource(String str)
    {
    	source = str;
    }
    
    public JsonObject getSourceObject()
    {
    	return obj;
    }
    
    public void setSourceObject(JsonObject json)
    {
    	obj = json;
    }
}
