package es.jodd.client;

public class ECException extends Exception 
{
	private static final long serialVersionUID = 1L;

	protected int httpStatus = 0;
    	protected String responseBody = null;
    
	public ECException()
	{
		super();
	}

	public ECException(String msg) 
	{
		super(msg);
	}

	public ECException(Throwable t) 
	{
		super(t);
	}

	public ECException(String msg, Throwable t) 
	{
		super(msg, t);
	}

	public void setHttpStatus(int code)
	{
		httpStatus = code;
	}
	
	public int getHttpStatus()
	{
		return httpStatus;
	}
	
	public void setResponseBody(String body)
	{
		responseBody = body;
	}
	
	public String getResponseBody()
	{
		return responseBody;
	}
}
