<Query Kind="Program">
  <NuGetReference>Flurl</NuGetReference>
  <NuGetReference>Flurl.Http</NuGetReference>
  <Namespace>Flurl</Namespace>
  <Namespace>Flurl.Http</Namespace>
  <Namespace>System.Threading.Tasks</Namespace>
</Query>

void Main()
{
	CbProRestApi api = new CbProRestApi();
	var products = api.GetProducts();
	//products.Dump();

	string productId = "REN-USD";
	DateTime start = DateTime.Now.AddDays(-10);
	DateTime end = DateTime.Now.AddDays(-9);
	int granularity = 3600; // hourly
	
	//var ph = 
	api.GetProductHistoricRates(productId, start, end, granularity);
	//ph.Dump();
}

// You can define other methods, fields, classes and namespaces here


// Retrieves market data from coinbase pro api
// https://docs.pro.coinbase.com/#market-data
public class CbProRestApi 
{
	public static string url_base = @"https://api.pro.coinbase.com";
	public static string endpoint_products() => @"products";
	public static string endpoint_product_historic_rates() => @$"products/REN-USD/candles";

	public DateTime last_call;
	
	
	public async Task<IEnumerable<Object>> GetProducts()
	{
		var products = await url_base
			.AppendPathSegment(endpoint_products())
			.GetJsonListAsync();

		return products.Where(x => x.quote_currency == "USD");
	}


	public async void GetProductHistoricRates(string productId, DateTime start, DateTime end, int granularitySec)
	{
		var isoStart = start.ToString("yyyy-MM-ddTHH:mm:ss.fffK");
		var isoEnd = end.ToString("yyyy-MM-ddTHH:mm:ss.fffK");
		
		var products = await url_base
			.AppendPathSegment("products/REN-USD/candles")
			//.SetQueryParam("start", isoStart)
			//.SetQueryParam("end", isoEnd)
			.SetQueryParam("granularity", granularitySec)
			.GetAsync();

		//		var products = await url_base
		//			.AppendPathSegment(endpoint_product_historic_rates(productId))
		//			.PostJsonAsync(new {
		//				start = 1618014900,
		//				end = 1618015380,
		//				granularity = granularity				
		//			});



		//products.Dump();
		//products.GetJsonAsync().Dump()
		products.ResponseMessage.Dump();
	}

}