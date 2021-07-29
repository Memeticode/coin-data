<Query Kind="Program">
  <NuGetReference>Microsoft.ML</NuGetReference>
  <NuGetReference>System.Data.SqlClient</NuGetReference>
  <Namespace>Microsoft.ML</Namespace>
  <Namespace>Microsoft.ML.Data</Namespace>
  <Namespace>System.Data.SqlClient</Namespace>
</Query>

void Main()
{
	var context = new Microsoft.ML.MLContext(seed: 0);

	//IDataView data = GetDataCsv(context);
	IDataView data = GetDataDb(context);
	DataOperationsCatalog.TrainTestData dataSplit = context.Data.TrainTestSplit(data, testFraction: 0.5);
	IDataView trainData = dataSplit.TrainSet;
	IDataView testData = dataSplit.TestSet;
	var features = dataSplit.TrainSet.Schema
		.Select(col => col.Name)
		.Where(colName => colName != "Label")
		.ToArray();


	var pipeline = context.Transforms.Concatenate("Features", features)
		.Append(context.BinaryClassification.Trainers.SdcaLogisticRegression())
		.AppendCacheCheckpoint(context);
		
	var model = pipeline.Fit(trainData);

	var predictions = model.Transform(testData);

	var metrics = context.BinaryClassification.Evaluate(predictions);
	
	//Console.WriteLine(metrics.ConfusionMatrix);
	metrics.Dump();
	

}

public static IDataView GetDataCsv(MLContext context)
{
	string path = @"C:\Users\thatc\source\coin-data\coinbase-data\data\FullDataSet1.csv";
	IDataView data = context.Data.LoadFromTextFile<CoinData>(path, hasHeader: true, separatorChar: ',');
	return data;
}

public static IDataView GetDataDb(MLContext context)
{
	
	string cnxn = @"DRIVER={SQL Server};SERVER={DESKTOP-BTN3TLD\TESQL};DATABASE={CoinData};Trusted_Connection=yes;";
	string cmnd = @"
		SELECT 
			is_gain_opportunity				
			, pct_change_est_avg_price			
			, pct_change_est_avg_trade_volume	
			, sma8_pct_diff						
			, sma21_pct_diff					
			, sma55_pct_diff					
			, sma89_pct_diff					
			, sma144_pct_diff					
			, btc_change_est_avg_price			
			, btc_change_est_avg_trade_volume	
			, btc_sma8_pct_diff					
			, btc_sma21_pct_diff				
			, btc_sma55_pct_diff				
			, btc_sma89_pct_diff				
			, btc_sma144_pct_diff				
			, btc_reg3_b						
			, btc_reg3_rsq						
			, btc_reg3_resid					
			, btc_reg8_b						
			, btc_reg8_rsq						
			, btc_reg8_resid					
			, btc_reg21_b						
			, btc_reg21_rsq						
			, btc_reg21_resid		
		FROM dbo.ModelData_1HrInterval_1DayHold_10PctGain2PctLoss 	
	";
	DatabaseLoader loader = context.Data.CreateDatabaseLoader<CoinData>();
	DatabaseSource dbSource = new DatabaseSource(SqlClientFactory.Instance, cnxn, cmnd);

	IDataView data = loader.Load(dbSource);
	
	
	return data;
}

// You can define other methods, fields, classes and namespaces here

// INPUT

public class CoinData
{
	[LoadColumn(0), ColumnName("Label")]
	public Boolean is_gain_opportunity;
	
	[LoadColumn(1)]
	public float pct_change_est_avg_price;

	[LoadColumn(2)]
	public float pct_change_est_avg_trade_volume;

	[LoadColumn(3)]
	public float sma8_pct_diff;

	[LoadColumn(4)]
	public float sma21_pct_diff;

	[LoadColumn(5)]
	public float sma55_pct_diff;

	[LoadColumn(6)]
	public float sma89_pct_diff;

	[LoadColumn(7)]
	public float sma144_pct_diff;

	[LoadColumn(8)]
	public float btc_change_est_avg_price;

	[LoadColumn(9)]
	public float btc_change_est_avg_trade_volume;

	[LoadColumn(10)]
	public float btc_sma8_pct_diff;

	[LoadColumn(11)]
	public float btc_sma21_pct_diff;

	[LoadColumn(12)]
	public float btc_sma55_pct_diff;

	[LoadColumn(13)]
	public float btc_sma89_pct_diff;

	[LoadColumn(14)]
	public float btc_sma144_pct_diff;

	[LoadColumn(15)]
	public float btc_reg3_b;

	[LoadColumn(16)]
	public float btc_reg3_rsq;

	[LoadColumn(17)]
	public float btc_reg3_resid;

	[LoadColumn(18)]
	public float btc_reg8_b;

	[LoadColumn(19)]
	public float btc_reg8_rsq;

	[LoadColumn(20)]
	public float btc_reg8_resid;

	[LoadColumn(21)]
	public float btc_reg21_b;

	[LoadColumn(22)]
	public float btc_reg21_rsq;

	[LoadColumn(23)]
	public float btc_reg21_resid;
}

// OUTPUT
public class CoinDataPrediction
{
	//public float Score { get; set; }

	[ColumnName("PredictedLabel")]
	public Boolean Prediction { get; set; }

	public float Probability { get; set; }
}



//public class CoinData
//{
//	public float pct_change_est_avg_price { get; set; }
//	public float pct_change_est_avg_trade_volume { get; set; }
//	public float sma8_pct_diff { get; set; }
//	public float sma21_pct_diff { get; set; }
//	public float sma55_pct_diff { get; set; }
//	public float sma89_pct_diff { get; set; }
//	public float sma144_pct_diff { get; set; }
//	public float btc_change_est_avg_price { get; set; }
//	public float btc_change_est_avg_trade_volume { get; set; }
//	public float btc_sma8_pct_diff { get; set; }
//	public float btc_sma21_pct_diff { get; set; }
//	public float btc_sma55_pct_diff { get; set; }
//	public float btc_sma89_pct_diff { get; set; }
//	public float btc_sma144_pct_diff { get; set; }
//	public float btc_reg3_b { get; set; }
//	public float btc_reg3_rsq { get; set; }
//	public float btc_reg3_resid { get; set; }
//	public float btc_reg8_b { get; set; }
//	public float btc_reg8_rsq { get; set; }
//	public float btc_reg8_resid { get; set; }
//	public float btc_reg21_b { get; set; }
//	public float btc_reg21_rsq { get; set; }
//	public float btc_reg21_resid { get; set; }
//}