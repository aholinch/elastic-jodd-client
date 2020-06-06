package es.jodd.client;

/**
 * ElasticSearch search results holds a list of search hits.
 * 
 * @author aholinc
 *
 */
public class SearchResults 
{
	/**
	 * The total number of hits found by the search.
	 */
	protected long total;

	/**
	 * The highest score matching the query.
	 */
	protected double maxScore;

	/**
	 * The array of hits actually returned by the search.
	 */
	protected SearchHit hits[];

	/**
	 * Default constructor.
	 */
	public SearchResults()
	{

	}

	/**
	 * Return the total number of hits.
	 * 
	 * @return
	 */
	public long getTotal()
	{
		return total;
	}

	/**
	 * Set the total number of hits.
	 * 
	 * @param tot
	 */
	public void setTotal(long tot)
	{
		total = tot;
	}

	/**
	 * Return the maximum score.
	 * 
	 * @return
	 */
	public double getMaxScore()
	{
		return maxScore;
	}

	/**
	 * Set the maximum score.
	 * 
	 * @param ms
	 */
	public void setMaxScore(double ms)
	{
		maxScore = ms;
	}

	/**
	 * Return the hits array.
	 * 
	 * @return
	 */
	public SearchHit[] getHits()
	{
		return hits;
	}

	/**
	 * Set the hits array.
	 * 
	 * @param hitArray
	 */
	public void setHits(SearchHit[] hitArray)
	{
		hits = hitArray;
	}
}
